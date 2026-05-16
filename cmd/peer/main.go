package main

import (
	"fmt"
	"go-api/internal/discovery"
	"go-api/internal/domain"
)

func main() {

	table := discovery.NewPeerTable()

	bootstrap := discovery.NewBootstrap([]domain.Peer{
		{ID: "peer1", Host: "127.0.01", Port: 50051, Heartbeat: 1, Alive: true},
		{ID: "peer2", Host: "127.0.01", Port: 50052, Heartbeat: 1, Alive: true},
	})

	bootstrap.Seed(table)

	for _, peer := range table.GetAll() {
		fmt.Printf("%s -> %s:%d \n", peer.ID, peer.Host, peer.Port)

	}

}
