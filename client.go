package main

import (
	"sync"
	"time"
)

type LogicalClient struct {
	store       KeyValueStore
	reportChans ReportingChans
	errorChan   chan WatchedErr
	ioWG        *sync.WaitGroup
}

func NewLogicalClient(store KeyValueStore, reportChans ReportingChans,
	errorChan chan WatchedErr, ioWG *sync.WaitGroup) *LogicalClient {
	return &LogicalClient{store: store, reportChans: reportChans, errorChan: errorChan, ioWG: ioWG}
}

func (lc *LogicalClient) Blast(depth int, ratio float64, existingData []string) {
	numWrites := float64(depth) * ratio
	numReads := float64(depth)

	lc.ioWG.Add(int(numReads + numWrites)) // set it here so we guarantee no race conditions

	// writes
	go func() {
		for i := 0; float64(i) < numWrites; i++ {
			time.Sleep(time.Duration(randomRange(0, 30)) * time.Millisecond)
			key := randSeq(KEY_SIZE)
			go timeTrack(lc.reportChans.write, lc.errorChan, func() WatchedErr {
				defer lc.ioWG.Done()
				return WatchedErr{err: lc.store.Set(key, "value"), opType: 0}
			})
		}
	}()

	// reads
	go func() {
		for i := 0; float64(i) < numReads; i++ {
			time.Sleep(time.Duration(randomRange(0, 30)) * time.Millisecond)
			key := existingData[randomRange(0, len(existingData))]
			go timeTrack(lc.reportChans.read, lc.errorChan, func() WatchedErr {
				defer lc.ioWG.Done()
				_, err := lc.store.Get(key)
				return WatchedErr{err: err, opType: 1}
			})
		}

	}()
}
