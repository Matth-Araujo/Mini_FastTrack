package discovery

import (
	"sync"

	"go-api/internal/domain"
)

type FileIndex struct {
	mu          sync.RWMutex
	byHash      map[string]domain.FileInfo
	peersByHash map[string]map[string]struct{}
	knownPeers  map[string]domain.Peer
}

func NewFileIndex() *FileIndex {
	return &FileIndex{
		byHash:      make(map[string]domain.FileInfo),
		peersByHash: make(map[string]map[string]struct{}),
		knownPeers:  make(map[string]domain.Peer),
	}
}

func (fi *FileIndex) Add(peerID string, file domain.FileInfo) {
	fi.mu.Lock()
	defer fi.mu.Unlock()

	fi.byHash[file.Checksum] = file
	if fi.peersByHash[file.Checksum] == nil {
		fi.peersByHash[file.Checksum] = make(map[string]struct{})
	}
	fi.peersByHash[file.Checksum][peerID] = struct{}{}
}

func (fi *FileIndex) AddPeer(peer domain.Peer) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.knownPeers[peer.ID] = peer
}

func (fi *FileIndex) GetByChecksum(checksum string) (domain.FileInfo, bool) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	file, ok := fi.byHash[checksum]
	return file, ok
}

func (fi *FileIndex) GetPeers(checksum string) []string {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	src := fi.peersByHash[checksum]
	result := make([]string, 0, len(src))
	for peerID := range src {
		result = append(result, peerID)
	}
	return result
}

func (fi *FileIndex) All() []domain.FileInfo {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	result := make([]domain.FileInfo, 0, len(fi.byHash))
	for _, file := range fi.byHash {
		result = append(result, file)
	}
	return result
}

func (fi *FileIndex) KnownPeers() []domain.Peer {
	fi.mu.RLock()
	defer fi.mu.RUnlock()

	result := make([]domain.Peer, 0, len(fi.knownPeers))
	for _, peer := range fi.knownPeers {
		result = append(result, peer)
	}
	return result
}
