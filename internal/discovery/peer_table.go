package discovery

import (
	"log"
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

	now := time.Now().Unix()
	existing, ok := pt.peers[peer.ID]

	if !ok {
		peer.LastSeen = now
		peer.Alive = true
		pt.peers[peer.ID] = peer
		return
	}

	if peer.Heartbeat > existing.Heartbeat {
		existing.Host = peer.Host
		existing.Port = peer.Port
		existing.Heartbeat = peer.Heartbeat
		existing.LastSeen = now
		existing.Alive = true
		pt.peers[peer.ID] = existing
		return
	}

	if peer.Heartbeat == existing.Heartbeat {
		existing.Host = peer.Host
		existing.Port = peer.Port
		existing.LastSeen = now
		existing.Alive = true
		pt.peers[peer.ID] = existing
		return
	}

	// heartbeat menor: ignora
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

	result := make([]domain.Peer, 0, len(pt.peers))
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
		if peer.Alive && now-peer.LastSeen > timeout {
			peer.Alive = false
			pt.peers[id] = peer
			log.Printf("[peer_table] peer %s marcado como falho", id)
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
			log.Printf("[peer_table] peer %s removido da tabela", id)
		}
	}
}

func (pt *PeerTable) MarkDead(id string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	peer, ok := pt.peers[id]
	if !ok {
		return
	}

	peer.Alive = false
	pt.peers[id] = peer
}

func (pt *PeerTable) GetByID(id string) (domain.Peer, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	peer, ok := pt.peers[id]
	return peer, ok
}
