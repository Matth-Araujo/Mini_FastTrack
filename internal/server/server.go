package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go-api/internal/discovery"
	"go-api/internal/domain"
	p2p "go-api/proto"

	"google.golang.org/grpc"
	gpeer "google.golang.org/grpc/peer"
)

const chunkSize = 32 * 1024

type P2PServer struct {
	p2p.UnimplementedP2PServiceServer
	table     *discovery.PeerTable
	fileIndex *discovery.FileIndex
	selfID    string
	host      string
	port      int
	filesDir  string
}

func NewP2PServer(table *discovery.PeerTable, fileIndex *discovery.FileIndex, selfID, host string, port int) *P2PServer {
	dir := filepath.Join("files", selfID)
	_ = os.MkdirAll(dir, 0755)

	return &P2PServer{
		table:     table,
		fileIndex: fileIndex,
		selfID:    selfID,
		host:      host,
		port:      port,
		filesDir:  dir,
	}
}

func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func (s *P2PServer) Ping(ctx context.Context, req *p2p.PingRequest) (*p2p.PingResponse, error) {
	if caller, ok := gpeer.FromContext(ctx); ok {
		log.Printf("Ping recebido de %s", caller.Addr.String())
	}
	return &p2p.PingResponse{Message: "pong"}, nil
}

func (s *P2PServer) GetPeers(ctx context.Context, req *p2p.GetPeersRequest) (*p2p.GetPeersResponse, error) {
	peers := s.table.GetAlive()
	resp := &p2p.GetPeersResponse{Peers: make([]*p2p.PeerInfo, 0, len(peers))}

	for _, peer := range peers {
		if peer.ID == s.selfID {
			continue
		}
		resp.Peers = append(resp.Peers, &p2p.PeerInfo{
			Id:        peer.ID,
			Host:      peer.Host,
			Port:      int32(peer.Port),
			Lastseen:  peer.LastSeen,
			Heartbeat: peer.Heartbeat,
			Alive:     peer.Alive,
		})
	}

	return resp, nil
}

func (s *P2PServer) RegisterPeer(ctx context.Context, req *p2p.RegisterPeerRequest) (*p2p.RegisterPeerResponse, error) {
	if req.GetPeer() == nil {
		return &p2p.RegisterPeerResponse{Ok: false, Message: "peer inválido"}, nil
	}

	peer := domain.Peer{
		ID:        req.GetPeer().GetId(),
		Host:      req.GetPeer().GetHost(),
		Port:      int(req.GetPeer().GetPort()),
		LastSeen:  time.Now().Unix(),
		Heartbeat: req.GetPeer().GetHeartbeat(),
		Alive:     true,
	}

	s.table.AddOrUpdate(peer)
	s.fileIndex.AddPeer(peer)

	selfPeer, _ := s.table.GetByID(s.selfID)
	return &p2p.RegisterPeerResponse{
		Ok:      true,
		Message: "peer registrado",
		Self: &p2p.PeerInfo{
			Id:        s.selfID,
			Host:      s.host,
			Port:      int32(s.port),
			Lastseen:  selfPeer.LastSeen,
			Heartbeat: selfPeer.Heartbeat,
			Alive:     true,
		},
	}, nil
}

func (s *P2PServer) ListFiles(ctx context.Context, req *p2p.ListFilesRequest) (*p2p.ListFilesResponse, error) {
	entries, err := os.ReadDir(s.filesDir)
	if err != nil {
		return &p2p.ListFilesResponse{}, nil
	}

	resp := &p2p.ListFilesResponse{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		path := filepath.Join(s.filesDir, entry.Name())
		checksum, err := checksumFile(path)
		if err != nil {
			continue
		}

		resp.Files = append(resp.Files, &p2p.FileInfo{
			Name:     entry.Name(),
			Size:     info.Size(),
			Checksum: checksum,
		})
	}

	return resp, nil
}

func (s *P2PServer) GetFileIndex(ctx context.Context, req *p2p.GetFileIndexRequest) (*p2p.GetFileIndexResponse, error) {
	allFiles := s.fileIndex.All()
	knownPeers := s.fileIndex.KnownPeers()

	peerMap := make(map[string]domain.Peer, len(knownPeers))
	for _, p := range knownPeers {
		peerMap[p.ID] = p
	}

	resp := &p2p.GetFileIndexResponse{}
	for _, f := range allFiles {
		peerIDs := s.fileIndex.GetPeers(f.Checksum)
		peers := make([]*p2p.PeerInfo, 0, len(peerIDs))

		for _, pid := range peerIDs {
			p, ok := peerMap[pid]
			if !ok {
				continue
			}
			peers = append(peers, &p2p.PeerInfo{
				Id:        p.ID,
				Host:      p.Host,
				Port:      int32(p.Port),
				Lastseen:  p.LastSeen,
				Heartbeat: p.Heartbeat,
				Alive:     p.Alive,
			})
		}

		resp.Files = append(resp.Files, &p2p.IndexedFile{
			Name:     f.Name,
			Size:     f.Size,
			Checksum: f.Checksum,
			Peers:    peers,
		})
	}

	return resp, nil
}

func (s *P2PServer) DownloadFile(req *p2p.DownloadFileRequest, stream p2p.P2PService_DownloadFileServer) error {
	filename := filepath.Base(req.GetFilename())
	path := filepath.Join(s.filesDir, filename)

	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("arquivo não encontrado: %s", filename)
	}
	defer f.Close()

	buf := make([]byte, chunkSize)
	var index int64

	for {
		n, err := f.Read(buf)
		if n > 0 {
			eof := err == io.EOF
			if sendErr := stream.Send(&p2p.FileChunk{
				Filename:   filename,
				Content:    buf[:n],
				ChunkIndex: index,
				Eof:        eof,
			}); sendErr != nil {
				return sendErr
			}
			index++
			if eof {
				break
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func StartGRPCServer(table *discovery.PeerTable, fileIndex *discovery.FileIndex, selfID, host string, port int) error {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	p2p.RegisterP2PServiceServer(grpcServer, NewP2PServer(table, fileIndex, selfID, host, port))

	log.Printf("gRPC server escutando em %s", addr)
	return grpcServer.Serve(lis)
}

func (s *P2PServer) SearchFiles(ctx context.Context, req *p2p.SearchFilesRequest) (*p2p.SearchFilesResponse, error) {
	query := strings.ToLower(req.GetQuery())
	allFiles := s.fileIndex.All()
	knownPeers := s.fileIndex.KnownPeers()

	peerMap := make(map[string]domain.Peer, len(knownPeers))
	for _, p := range knownPeers {
		peerMap[p.ID] = p
	}

	resp := &p2p.SearchFilesResponse{}

	for _, f := range allFiles {
		if strings.Contains(strings.ToLower(f.Name), query) {
			peerIDs := s.fileIndex.GetPeers(f.Checksum)
			peers := make([]*p2p.PeerInfo, 0, len(peerIDs))

			for _, pid := range peerIDs {
				if p, ok := peerMap[pid]; ok {
					peers = append(peers, &p2p.PeerInfo{
						Id:   p.ID,
						Host: p.Host,
						Port: int32(p.Port),
					})
				}
			}

			resp.Results = append(resp.Results, &p2p.IndexedFile{
				Name:     f.Name,
				Size:     f.Size,
				Checksum: f.Checksum,
				Peers:    peers,
			})
		}
	}

	return resp, nil
}
