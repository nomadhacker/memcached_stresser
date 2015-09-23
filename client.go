package main

import (
	"sync"
	"time"
)

type LogicalClient struct {
	store      KeyValueStore
	reportChan chan time.Duration
	errorChan  chan WatchedErr
	ioWG       *sync.WaitGroup
}

func NewLogicalClient(store KeyValueStore, reportChan chan time.Duration,
	errorChan chan WatchedErr, ioWG *sync.WaitGroup) *LogicalClient {
	return &LogicalClient{store: store, reportChan: reportChan, errorChan: errorChan, ioWG: ioWG}
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
			timeTrack(lc.reportChan, lc.errorChan, func() WatchedErr {
				return WatchedErr{err: ls.store.Set(key, "value"), opType: 0}
			})
		}
		lc.ioWG.Done()
	}()

	// reads
	go func() {
		for i := 0; float64(i) < numReads; i++ {
			time.Sleep(time.Duration(randomRange(0, 30)) * time.Millisecond)
			key := existingData[randomRange(0, len(startingData))]
			timeTrack(lc.reportChan, lc.errorChan, func() WatchedErr {
				_, err := ls.store.Get(key)
				return WatchedErr{err: err, opType: 1}
			})
		}
		lc.ioWG.Done()
	}()
}
