package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WatchedErr struct {
	err    error
	opType int
}

const (
	KEY_SIZE = 64
	VAL_SIZE = 12
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func main() {
	ratio := flag.Float64("ratio", 0.1, "ratio, in decimal form, of writes/reads")
	factor := flag.Float64("factor", 1000, "volumizing factor")
	storeURIstring := flag.String("store", "localhost:11211,localhost:5000", "store host URIs separated by comma")
	startingRecordSize := flag.Int64("start", 10000, "starting record size")
	numClients := flag.Int64("clients", 80, "amount of logical clients to emulate")

	flag.Parse()

	rand.Seed(time.Now().Unix())

	numWrites := *factor * *ratio // writes is always lower
	numReads := *factor

	startingData := genStartingData(*startingRecordSize)

	var ioWG sync.WaitGroup
	var reportingWG sync.WaitGroup
	ioWG.Add(int(numReads + numWrites)) // set it here so we guarantee no race conditions

	writeReportChan := make(chan time.Duration, 999)
	readReportChan := make(chan time.Duration, 999)
	errorChan := make(chan WatchedErr, 999)

	reportSignal := make(chan struct{})

	// Build sharded memcached struct
	shards := strings.Split(*storeURIstring, ",")
	store := NewShardedMemcachedKVS(shards)

	err := store.Flush()
	if err != nil {
		fmt.Println(err.Error())
		log.Fatal("Failed to flush existing keys")
	}

	reportChans := ReportingChans{write: writeReportChan, read: readReportChan}

	emulatedNodes := NewLogicalCluster(int(*numClients), store, reportChans, errorChan, &ioWG)

	// Load starting data, report metrics on timing
	// (essentially a sequential write benchmark)
	startingDataStart := time.Now()
	err = writeStartingData(store, startingData)
	startingDataElapsed := time.Since(startingDataStart)

	if err != nil {
		fmt.Println(err)
		log.Fatal("Starting data failed to load")
	}

	fmt.Println("Loaded " + strconv.Itoa(int(*startingRecordSize)) + " starting records in: ")
	fmt.Println(startingDataElapsed)

	// reporting loop
	reportingWG.Add(1)
	go func() {
		globalStart := time.Now()
		var writes []time.Duration
		var reads []time.Duration
	REPORTLOOP:
		for {
			select {
			case <-reportSignal:
				break REPORTLOOP
			case metric := <-writeReportChan:
				writes = append(writes, metric)
			case metric := <-readReportChan:
				reads = append(reads, metric)
			}
		}
		globalDelta := time.Since(globalStart)

		// drain channels
		for i := 0; i < len(writeReportChan); i++ {
			metric := <-writeReportChan
			writes = append(writes, metric)
		}
		for i := 0; i < len(readReportChan); i++ {
			metric := <-readReportChan
			writes = append(reads, metric)
		}

		fmt.Printf("\nCompleted %d total writes in roughly %s \n", len(writes), globalDelta)
		floatWrites := durationToMilliseconds(writes)
		avgWriteTime := findAverage(floatWrites)
		fmt.Printf("Average write time elapsed: %g ms \n", avgWriteTime)
		fmt.Printf("Average write time std dev: %g ms \n", findStdDev(avgWriteTime, floatWrites))
		// go buildHistogram(floatWrites, "write_histo")

		fmt.Printf("\nCompleted %d total reads in roughly %s \n", len(reads), globalDelta)
		floatReads := durationToMilliseconds(reads)
		avgReadTime := findAverage(floatReads)
		fmt.Printf("Average read time elapsed: %g ms \n", avgReadTime)
		fmt.Printf("Average read time std dev: %g ms \n", findStdDev(avgReadTime, floatReads))
		// go buildHistogram(floatReads, "read_histo")

		reportingWG.Done()
	}()

	reportingWG.Add(1)
	go func() {
		var writeErrorCount int
		var readErrorCount int
		var errors []error
	ERRORLOOP:
		for {
			select {
			case <-reportSignal:
				break ERRORLOOP
			case err := <-errorChan:
				if err.err != nil {
					errors = append(errors, err.err)
					if err.opType == 0 {
						writeErrorCount = writeErrorCount + 1
					} else {
						readErrorCount = readErrorCount + 1
					}
				}
			}
		}
		// drain channel
		for i := 0; i < len(errorChan); i++ {
			err := <-errorChan
			errors = append(errors, err.err)
		}
		errorCount := len(errors)
		fmt.Println("\nTotal Errors: " + strconv.Itoa(errorCount))
		errorCounts := findMostCommonErrors(errors)
		writeErrorPercentage := (float64(writeErrorCount) / float64(writeErrorCount+readErrorCount)) * 100
		readErrorPercentage := (float64(readErrorCount) / float64(writeErrorCount+readErrorCount)) * 100
		fmt.Printf("\nError distribution: Writes %5.3f%%, Reads %5.3f%% \n", writeErrorPercentage, readErrorPercentage)
		for key, value := range errorCounts {
			fmt.Println("Error: \"" + key + "\" Count: " + strconv.Itoa(value))
		}
		reportingWG.Done()
	}()

	emulatedNodes.BlastAll(int(*factor), *ratio, startingData)

	ioWG.Wait() // wait for our reads/writes to finish
	reportSignal <- struct{}{}
	reportSignal <- struct{}{}
	reportingWG.Wait() // wait for our reporting to finish
}

func findMostCommonErrors(list []error) map[string]int {
	count := make(map[string]int)
	for _, err := range list {
		i, _ := count[err.Error()]
		count[err.Error()] = i + 1
	}
	return count
}

func randomRange(min, max int) int {
	return rand.Intn(max-min) + min
}

func timeTrack(reportChan chan time.Duration, errorChan chan WatchedErr, doFunc func() WatchedErr) {
	start := time.Now()
	err := doFunc()
	elapsed := time.Since(start)
	if err.err != nil {
		if err.err == (io.EOF) {
			timeTrack(reportChan, errorChan, doFunc)
		} else {
			errorChan <- err
		}
	} else {
		reportChan <- elapsed
	}
}

// http://stackoverflow.com/a/22892986/681342
func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func genStartingData(numRecords int64) []string {
	var testData []string
	var i int64
	for i = 0; i < numRecords; i++ {
		testData = append(testData, randSeq(KEY_SIZE))
	}
	return testData
}

func writeStartingData(store KeyValueStore, data []string) error {
	for _, hash := range data {
		err := store.Set(hash, "asdf")
		if err != nil {
			return err
		}
	}
	return nil
}

func durationToMilliseconds(list []time.Duration) []float64 {
	var result []float64
	for _, val := range list {
		result = append(result, val.Seconds()*1000.0)
	}
	return result
}

func findAverage(list []float64) float64 {
	var total float64
	for _, dataPoint := range list {
		total = total + dataPoint
	}
	return total / float64(len(list))
}

func findStdDev(average float64, list []float64) float64 {
	var variances []float64
	for _, dataPoint := range list {
		variances = append(variances, math.Pow(dataPoint-average, 2))
	}
	return math.Sqrt(findAverage(variances))
}
