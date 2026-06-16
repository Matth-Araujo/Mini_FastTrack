package discovery

import (
	"log"
	"math/rand"
	"time"

	"go-api/internal/client"
	"go-api/internal/domain"
)

type GossipManager struct {
	table         *PeerTable
	client        *client.P2PClient
	self          domain.Peer
	interval      time.Duration
	fanout        int
	failTimeout   int64
	removeTimeout int64
}

func NewGossipManager(
	table *PeerTable,
	client *client.P2PClient,
	self domain.Peer,
	interval time.Duration,
	fanout int,
	failTimeout int64,
	removeTimeout int64,
) *GossipManager {
	return &GossipManager{
		table:         table,
		client:        client,
		self:          self,
		interval:      interval,
		fanout:        fanout,
		failTimeout:   failTimeout,
		removeTimeout: removeTimeout,
	}
}

func (g *GossipManager) Start() {
	ticker := time.NewTicker(g.interval)
	defer ticker.Stop()

	for range ticker.C {
		g.tick()
	}
}

func (g *GossipManager) tick() {
	g.bumpSelfHeartbeat()
	g.table.MarkFailed(g.failTimeout)
	g.table.RemoveDead(g.removeTimeout)

	targets := g.pickTargets()
	log.Printf("[%s][gossip] targets=%d", g.self.ID, len(targets))

	for _, peer := range targets {
		addr := client.Address(peer.Host, peer.Port)
		log.Printf("[%s][gossip] consultando %s (%s) hb=%d", g.self.ID, peer.ID, addr, peer.Heartbeat)

		remotePeers, err := g.client.GetPeers(addr)
		if err != nil {
			log.Printf("[%s][gossip] peer %s (%s) indisponível: conexão recusada ou serviço não está ativo",
				g.self.ID, peer.ID, addr)
			log.Printf("[%s][gossip] detalhe técnico: %v", g.self.ID, err)
			g.table.MarkDead(peer.ID)
			continue
		}

		log.Printf("[%s][gossip] recebeu %d peers de %s", g.self.ID, len(remotePeers), peer.ID)

		_, _ = g.client.RegisterPeer(addr, g.self)

		g.merge(remotePeers)
	}
}

func (g *GossipManager) bumpSelfHeartbeat() {
	g.self.Heartbeat++
	g.self.LastSeen = time.Now().Unix()
	g.self.Alive = true
	g.table.AddOrUpdate(g.self)
	log.Printf("[%s][gossip] self heartbeat=%d", g.self.ID, g.self.Heartbeat)
}

func (g *GossipManager) merge(peers []domain.Peer) {
	for _, peer := range peers {
		if peer.ID == g.self.ID {
			continue
		}
		log.Printf("[%s][gossip] merge peer=%s hb=%d alive=%v", g.self.ID, peer.ID, peer.Heartbeat, peer.Alive)
		g.table.AddOrUpdate(peer)
	}
}

func (g *GossipManager) pickTargets() []domain.Peer {
	alive := g.table.GetAlive()

	candidates := make([]domain.Peer, 0, len(alive))
	for _, peer := range alive {
		if peer.ID != g.self.ID {
			candidates = append(candidates, peer)
		}
	}

	if len(candidates) <= g.fanout {
		return candidates
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	return candidates[:g.fanout]
}
