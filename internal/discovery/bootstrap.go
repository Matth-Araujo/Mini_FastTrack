package discovery

import (
	"go-api/internal/domain"
)

type Bootsatrap struct {
	InitialPeers []domain.Peer
}

func NewBootstrap(inicialPeers []domain.Peer) *Bootsatrap {
	return &Bootsatrap{
		InitialPeers: inicialPeers,
	}
}

func (b *Bootsatrap) Seed(table *PeerTable) {
	for _, peer := range b.InitialPeers {
		table.AddOrUpdate(peer)
	}
}
