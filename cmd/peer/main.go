package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"go-api/internal/client"
	"go-api/internal/discovery"
	"go-api/internal/domain"
	"go-api/internal/server"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("uso: go run ./cmd/peer <peerID> <host> <port> [bootstrapHost:port,bootstrapHost:port]")
	}

	peerID := os.Args[1]
	host := os.Args[2]

	port, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("porta inválida: %v", err)
	}

	var bootstrapPeers []domain.Peer
	if len(os.Args) >= 5 && strings.TrimSpace(os.Args[4]) != "" {
		bootstrapPeers = parseBootstrapPeers(os.Args[4])
	}

	table := discovery.NewPeerTable()
	fileIndex := discovery.NewFileIndex()

	self := domain.Peer{
		ID:        peerID,
		Host:      host,
		Port:      port,
		LastSeen:  time.Now().Unix(),
		Heartbeat: 1,
		Alive:     true,
	}

	table.AddOrUpdate(self)
	fileIndex.AddPeer(self)

	go func() {
		if err := server.StartGRPCServer(table, fileIndex, self.ID, self.Host, self.Port); err != nil {
			log.Fatalf("erro ao subir gRPC server: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	grpcClient := client.NewP2PClient(3 * time.Second)
	selfFiles, err := grpcClient.ListFiles(client.Address(self.Host, self.Port))
	if err != nil {
		log.Printf("[%s] erro ao listar arquivos locais: %v", self.ID, err)
	} else {
		for _, f := range selfFiles {
			fileIndex.Add(self.ID, f)
		}
	}

	for _, peer := range bootstrapPeers {
		addr := client.Address(peer.Host, peer.Port)

		bootstrapSelf, err := grpcClient.RegisterPeer(addr, self)
		if err != nil {
			log.Printf("[%s] erro ao registrar no bootstrap %s: %v", self.ID, addr, err)
			continue
		}

		if bootstrapSelf != nil {
			table.AddOrUpdate(*bootstrapSelf)
			fileIndex.AddPeer(*bootstrapSelf)
		}

		remotePeers, err := grpcClient.GetPeers(addr)
		if err != nil {
			log.Printf("[%s] erro ao buscar peers de bootstrap %s: %v", self.ID, addr, err)
			continue
		}

		for _, p := range remotePeers {
			table.AddOrUpdate(p)
			fileIndex.AddPeer(p)
		}

		log.Printf("[%s] bootstrap inicial com %s trouxe %d peers", self.ID, addr, len(remotePeers))
	}

	alivePeers := table.GetAlive()
	for _, p := range alivePeers {
		if p.ID == self.ID || p.Host == "" || p.Port == 0 {
			continue
		}

		fileIndex.AddPeer(p)

		files, err := grpcClient.ListFiles(client.Address(p.Host, p.Port))
		if err != nil {
			log.Printf("[%s] erro ao listar arquivos de %s: %v", self.ID, p.ID, err)
			continue
		}

		for _, f := range files {
			fileIndex.Add(p.ID, f)
		}
	}

	gossip := discovery.NewGossipManager(
		table,
		grpcClient,
		self,
		5*time.Second,
		2,
		60,
		180,
	)

	go gossip.Start()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			for _, p := range table.GetAlive() {
				if p.ID == self.ID || p.Host == "" || p.Port == 0 {
					continue
				}

				fileIndex.AddPeer(p)

				files, err := grpcClient.ListFiles(client.Address(p.Host, p.Port))
				if err != nil {
					log.Printf("[%s] erro ao atualizar índice de %s: %v", self.ID, p.ID, err)
					continue
				}

				for _, f := range files {
					fileIndex.Add(p.ID, f)
				}
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			alivePeers := table.GetAlive()
			log.Printf("[%s] peers ativos conhecidos: %d", self.ID, len(alivePeers))
			for _, p := range alivePeers {
				log.Printf("[%s] -> %s %s:%d heartbeat=%d alive=%v last_seen=%d",
					self.ID, p.ID, p.Host, p.Port, p.Heartbeat, p.Alive, p.LastSeen)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	log.Printf("[%s] peer iniciado em %s:%d", self.ID, self.Host, self.Port)
	<-sigCh
	log.Printf("[%s] encerrando peer...", self.ID)
}

func parseBootstrapPeers(raw string) []domain.Peer {
	parts := strings.Split(raw, ",")
	peers := make([]domain.Peer, 0, len(parts))

	for _, item := range parts {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		hostPort := strings.Split(item, ":")
		if len(hostPort) != 2 {
			log.Printf("bootstrap inválido ignorado: %s", item)
			continue
		}

		port, err := strconv.Atoi(strings.TrimSpace(hostPort[1]))
		if err != nil {
			log.Printf("porta bootstrap inválida ignorada: %s", item)
			continue
		}

		peers = append(peers, domain.Peer{
			ID:        "",
			Host:      strings.TrimSpace(hostPort[0]),
			Port:      port,
			LastSeen:  0,
			Heartbeat: 0,
			Alive:     false,
		})
	}

	return peers
}
