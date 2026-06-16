package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"go-api/internal/domain"
	p2p "go-api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type P2PClient struct {
	timeout time.Duration
}

func NewP2PClient(timeout time.Duration) *P2PClient {
	return &P2PClient{
		timeout: timeout,
	}
}

func (c *P2PClient) dial(target string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func Address(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func (c *P2PClient) Ping(target string) error {
	conn, err := c.dial(target)
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	_, err = cli.Ping(ctx, &p2p.PingRequest{})
	return err
}

func (c *P2PClient) GetPeers(target string) ([]domain.Peer, error) {
	conn, err := c.dial(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := cli.GetPeers(ctx, &p2p.GetPeersRequest{})
	if err != nil {
		return nil, err
	}

	result := make([]domain.Peer, 0, len(resp.Peers))
	for _, peer := range resp.Peers {
		result = append(result, domain.Peer{
			ID:        peer.GetId(),
			Host:      peer.GetHost(),
			Port:      int(peer.GetPort()),
			LastSeen:  peer.GetLastseen(),
			Heartbeat: peer.GetHeartbeat(),
			Alive:     peer.GetAlive(),
		})
	}
	return result, nil
}

func (c *P2PClient) RegisterPeer(target string, peer domain.Peer) (*domain.Peer, error) {
	conn, err := c.dial(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := cli.RegisterPeer(ctx, &p2p.RegisterPeerRequest{
		Peer: &p2p.PeerInfo{
			Id:        peer.ID,
			Host:      peer.Host,
			Port:      int32(peer.Port),
			Lastseen:  peer.LastSeen,
			Heartbeat: peer.Heartbeat,
			Alive:     peer.Alive,
		},
	})
	if err != nil {
		return nil, err
	}

	if !resp.GetOk() {
		return nil, fmt.Errorf("falha ao registrar peer: %s", resp.GetMessage())
	}

	if s := resp.GetSelf(); s != nil {
		bootstrapPeer := &domain.Peer{
			ID:        s.GetId(),
			Host:      s.GetHost(),
			Port:      int(s.GetPort()),
			LastSeen:  time.Now().Unix(),
			Heartbeat: s.GetHeartbeat(),
			Alive:     true,
		}
		return bootstrapPeer, nil
	}

	return nil, nil
}

func (c *P2PClient) ListFiles(target string) ([]domain.FileInfo, error) {
	conn, err := c.dial(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := cli.ListFiles(ctx, &p2p.ListFilesRequest{})
	if err != nil {
		return nil, err
	}

	result := make([]domain.FileInfo, 0, len(resp.Files))
	for _, f := range resp.Files {
		result = append(result, domain.FileInfo{
			Name:     f.GetName(),
			Size:     f.GetSize(),
			Checksum: f.GetChecksum(),
		})
	}
	return result, nil
}

func (c *P2PClient) DownloadFile(target, filename, destDir, expectedChecksum string) error {
	conn, err := c.dial(target)
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := cli.DownloadFile(ctx, &p2p.DownloadFileRequest{Filename: filename})
	if err != nil {
		return err
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return err
	}

	destPath := filepath.Join(destDir, filepath.Base(filename))
	f, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer f.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := f.Write(chunk.GetContent()); err != nil {
			return err
		}
		if chunk.GetEof() {
			break
		}
	}

	if expectedChecksum != "" {
		if err := f.Close(); err != nil {
			return err
		}

		h := sha256.New()
		rf, err := os.Open(destPath)
		if err != nil {
			return err
		}
		defer rf.Close()

		if _, err := io.Copy(h, rf); err != nil {
			return err
		}

		got := hex.EncodeToString(h.Sum(nil))
		if got != expectedChecksum {
			_ = os.Remove(destPath)
			return fmt.Errorf("checksum inválido: esperado %s, obtido %s", expectedChecksum, got)
		}
	}

	log.Printf("arquivo salvo em %s", destPath)
	return nil
}

func (c *P2PClient) GetFileIndex(target string) ([]domain.IndexedFile, error) {
	conn, err := c.dial(target)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := p2p.NewP2PServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	resp, err := cli.GetFileIndex(ctx, &p2p.GetFileIndexRequest{})
	if err != nil {
		return nil, err
	}

	result := make([]domain.IndexedFile, 0, len(resp.Files))
	for _, f := range resp.Files {
		peers := make([]domain.Peer, 0, len(f.GetPeers()))
		for _, p := range f.GetPeers() {
			peers = append(peers, domain.Peer{
				ID:        p.GetId(),
				Host:      p.GetHost(),
				Port:      int(p.GetPort()),
				LastSeen:  p.GetLastseen(),
				Heartbeat: p.GetHeartbeat(),
				Alive:     p.GetAlive(),
			})
		}

		result = append(result, domain.IndexedFile{
			Name:     f.GetName(),
			Size:     f.GetSize(),
			Checksum: f.GetChecksum(),
			Peers:    peers,
		})
	}

	return result, nil
}
