package main

import (
	"sync"
	"time"
)

type LogicalClientCluster struct {
	clients []*LogicalClient
}

func NewLogicalCluster(numClients int, store KeyValueStore, reportChan chan time.Duration,
	errorChan chan WatchedErr, ioWG *sync.WaitGroup) *LogicalClientCluster {

	lc := &LogicalClientCluster{}
	for i := 0; i < numClients; i++ {
		lc.clients = append(lc.clients, NewLogicalClient(store, reportChan, errorChan, ioWG))
	}

	return lc
}

func (lc *LogicalClientCluster) BlastAll(depth int, ratio float64, existingData []string) {
	for _, each := range lc.clients {
		each.Blast(depth, ratio, existingData)
	}
}
