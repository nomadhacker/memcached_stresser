package main

import (
	"sync"
	"time"
)

type LogicalClientCluster struct {
	clients []*LogicalClient
}

type ReportingChans struct {
	read  chan time.Duration
	write chan time.Duration
}

func NewLogicalCluster(numClients int, store KeyValueStore, reportChans ReportingChans,
	errorChan chan WatchedErr, ioWG *sync.WaitGroup) *LogicalClientCluster {

	lc := &LogicalClientCluster{}
	for i := 0; i < numClients; i++ {
		lc.clients = append(lc.clients, NewLogicalClient(store, reportChans, errorChan, ioWG))
	}

	return lc
}

func (lc *LogicalClientCluster) BlastAll(depth int, ratio float64, existingData []string) {
	for _, each := range lc.clients {
		each.Blast(depth, ratio, existingData)
	}
}
