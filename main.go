package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

const (
	KEY_SIZE = 64
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func main() {
	ratio := flag.Float64("ratio", 0.1, "ratio, in decimal form, of writes/reads")
	factor := flag.Float64("factor", 1000, "volumizing factor")
	storeURIstring := flag.String("store", "localhost:11211,localhost:5000", "store host URIs separated by comma")
	startingRecordSize := flag.Int64("start", 10000, "starting record size")

	flag.Parse()

	rand.Seed(time.Now().Unix())

	numWrites := *factor * *ratio // writes is always lower
	numReads := *factor

	startingData := genStartingData(*startingRecordSize)

	var wg sync.WaitGroup
	var wg2 sync.WaitGroup

	writeReportChan := make(chan time.Duration, 999)
	readReportChan := make(chan time.Duration, 999)
	errorChan := make(chan error, 999)

	// reporting loop
	reportSignal := make(chan struct{})
	go func() {
		wg2.Add(1)
		fmt.Println("here")
		globalStart := time.Now()
		var writes []time.Duration
		var reads []time.Duration
		for {
			select {
			case <-reportSignal:
				break
			case metric := <-writeReportChan:
				writes = append(writes, metric)
			case metric := <-readReportChan:
				reads = append(reads, metric)
			}
		}
		globalDelta := time.Since(globalStart)

		fmt.Println("Completed %s total writes in roughly %s", len(writes), globalDelta)
		floatWrites := durationToNanosecondsMap(writes)
		avgWriteTime := findAverage(floatWrites)
		fmt.Println("Average write time elapsed: %s", avgWriteTime)
		fmt.Println("Average write time std dev: %s", findStdDev(avgWriteTime, floatWrites))

		fmt.Println("Completed %s total reads in roughly %s", len(reads), globalDelta)
		floatReads := durationToNanosecondsMap(reads)
		avgReadTime := findAverage(floatReads)
		fmt.Println("Average read time elapsed: %s", avgReadTime)
		fmt.Println("Average read time std dev: %s", findStdDev(avgReadTime, floatReads))

		fmt.Println("here2")
		wg2.Done()
	}()

	go func() {
		wg2.Add(1)
		var errors []error
		for {
			select {
			case <-reportSignal:
				break
			case err := <-errorChan:
				fmt.Println(err.Error())
				errors = append(errors, err)
			}
		}
		errorCount := len(errors)
		fmt.Println("Total Errors: " + strconv.Itoa(errorCount))
		wg2.Done()
	}()

	mc := memcache.New(strings.Split(*storeURIstring, ",")...)
	err := writeStartingData(mc, startingData)

	if err != nil {
		fmt.Println(err)
		log.Fatal("Starting data failed to load")
	}

	for i := 0; float64(i) < numWrites; i++ {
		go func() {
			wg.Add(1)
			newKey := randSeq(KEY_SIZE)
			item := &memcache.Item{Key: newKey, Value: []byte("1")}
			startingData = append(startingData, newKey)
			timeTrack(writeReportChan, errorChan, func() error {
				return mc.Set(item)
			})
			wg.Done()
		}()
	}

	for i := 0; float64(i) < numReads; i++ {
		go func() {
			wg.Add(1)
			key := startingData[randomRange(0, len(startingData))]
			timeTrack(readReportChan, errorChan, func() error {
				result, err := mc.Get(key)
				if err != nil {
					return err
				} else if string(result.Value) != "1" {
					return errors.New("Get op returned a non \"1\" result")
				}
				return nil
			})
			wg.Done()
		}()
	}
	wg.Wait() // wait for our reads/writes to finish
	reportSignal <- struct{}{}
	reportSignal <- struct{}{}
	wg2.Done() // wait for our reporting to finish
	fmt.Println("main exiting")
}

func randomRange(min, max int) int {
	return rand.Intn(max-min) + min
}

func timeTrack(reportChan chan time.Duration, errorChan chan error, doFunc func() error) {
	start := time.Now()
	err := doFunc()
	elapsed := time.Since(start)
	if err != nil {
		errorChan <- err
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

func writeStartingData(mc *memcache.Client, data []string) error {
	for _, hash := range data {
		err := mc.Set(&memcache.Item{Key: hash, Value: []byte("1")})
		if err != nil {
			return err
		}
	}
	return nil
}

func durationToNanosecondsMap(list []time.Duration) []float64 {
	var result []float64
	for _, val := range list {
		result = append(result, float64(val.Nanoseconds()))
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
	return findAverage(variances)
}