package main

import (
	"flag"
	"fmt"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/graft"
)

func main() {
	var (
		logPath     = flag.String("path", "./graft.log", "Raft log path")
		clusterName = flag.String("cluster", "cluster", "Cluster name")
		clusterSize = flag.Int("size", 3, "Cluster size")
		natsAddr    = flag.String("nats", nats.DefaultURL, "NATS address")
	)
	flag.Parse()

	var (
		opts = &nats.DefaultOptions
		ci   = graft.ClusterInfo{Name: *clusterName, Size: *clusterSize}
	)
	opts.Url = *natsAddr
	rpc, err := graft.NewNatsRpc(opts)
	if err != nil {
		panic(err)
	}

	var (
		errC         = make(chan error)
		stateChangeC = make(chan graft.StateChange)
		handler      = graft.NewChanHandler(stateChangeC, errC)
	)

	node, err := graft.New(ci, handler, rpc, *logPath)
	if err != nil {
		panic(err)
	}
	defer node.Close()

	handleState(node.State())

	for {
		select {
		case change := <-stateChangeC:
			handleState(change.To)
		case err := <-errC:
			fmt.Printf("Error: %s\n", err)
		}
	}
}

func handleState(state graft.State) {
	switch state {
	case graft.LEADER:
		fmt.Println("***Becoming leader***")
	case graft.FOLLOWER:
		fmt.Println("***Becoming follower***")
	case graft.CANDIDATE:
		fmt.Println("***Becoming candidate***")
	case graft.CLOSED:
		return
	default:
		panic(fmt.Sprintf("Unknown state: %s", state))
	}
}
