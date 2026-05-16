package discovery

import (
	"sync"
	"time"

	"go-api/internal/domain"
)

type PeerTable struct {
	mu    sync.RWMutex
	peers map[string]domain.Peer
}

func NewPeerTable() *PeerTable {
	return &PeerTable{
		peers: make(map[string]domain.Peer),
	}
}

func (pt *PeerTable) AddOrUpdate(peer domain.Peer) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	existing, ok := pt.peers[peer.ID]
	if !ok || peer.Heartbeat >= existing.Heartbeat {
		peer.LastSeen = time.Now().Unix()
		peer.Alive = true
		pt.peers[peer.ID] = peer
	}
}

func (pt *PeerTable) GetAll() []domain.Peer {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	result := make([]domain.Peer, 0, len(pt.peers))
	for _, peer := range pt.peers {
		result = append(result, peer)
	}
	return result
}

func (pt *PeerTable) GetAlive() []domain.Peer {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	var result []domain.Peer
	for _, peer := range pt.peers {
		if peer.Alive {
			result = append(result, peer)
		}
	}
	return result
}

func (pt *PeerTable) MarkFailed(timeout int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	now := time.Now().Unix()
	for id, peer := range pt.peers {
		if now-peer.LastSeen > timeout {
			peer.Alive = false
			pt.peers[id] = peer
		}
	}
}

func (pt *PeerTable) RemoveDead(removeAfter int64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	now := time.Now().Unix()
	for id, peer := range pt.peers {
		if !peer.Alive && now-peer.LastSeen > removeAfter {
			delete(pt.peers, id)
		}
	}
}
