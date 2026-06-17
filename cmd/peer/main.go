package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
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

	// Configuração de log para não sujar o terminal
	os.MkdirAll("logs", 0755)
	logPath := filepath.Join("logs", peerID+".log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("erro ao abrir arquivo de log: %v\n", err)
		os.Exit(1)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

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

	go func() {
		reader := bufio.NewReader(os.Stdin)
		time.Sleep(2 * time.Second)
		fmt.Println("\n==========================================")
		fmt.Printf("  Go FastTrack — %s\n", self.ID)
		fmt.Printf("  IP: %s  Porta: %d\n", self.Host, self.Port)
		fmt.Println("==========================================")
		fmt.Println("  Digite 'help' para ver os comandos.")

		for {
			fmt.Print("\nfasttrack> ")
			input, _ := reader.ReadString('\n')
			input = strings.TrimSpace(input)
			if input == "" {
				continue
			}

			parts := strings.Split(input, " ")
			cmd := strings.ToLower(parts[0])
			args := parts[1:]

			switch cmd {
			case "help":
				fmt.Println("  peers                               -> lista peers ativos na rede")
				fmt.Println("  myfiles                             -> lista os seus arquivos locais")
				fmt.Println("  files <peer_id>                     -> lista arquivos de um peer específico")
				fmt.Println("  search <nome>                       -> busca arquivo por nome")
				fmt.Println("  download <host:porta> <arquivo>     -> baixa um arquivo de um peer")
				fmt.Println("  exit                                -> sai da rede")

			case "peers":
				ativos := table.GetAlive()
				fmt.Printf("\n  %-10s %-15s %-6s\n", "ID", "HOST", "PORTA")
				fmt.Println("  " + strings.Repeat("-", 35))
				for _, p := range ativos {
					fmt.Printf("  %-10s %-15s %-6d\n", p.ID, p.Host, p.Port)
				}

			case "myfiles":
				localAddr := client.Address(self.Host, self.Port)
				arquivos, err := grpcClient.ListFiles(localAddr)
				if err != nil {
					fmt.Printf("  Erro ao listar arquivos locais: %v\n", err)
					continue
				}
				if len(arquivos) == 0 {
					fmt.Println("  Nenhum arquivo encontrado na sua pasta local.")
					continue
				}
				fmt.Printf("\n  %-25s %-10s %-30s\n", "NOME", "TAMANHO", "CHECKSUM")
				fmt.Println("  " + strings.Repeat("-", 70))
				for _, f := range arquivos {
					fmt.Printf("  %-25s %-10d %s...\n", f.Name, f.Size, f.Checksum[:16])
				}

			case "files":
				if len(args) == 0 {
					fmt.Println("  Uso: files <peer_id>")
					continue
				}
				targetID := args[0]
				allFiles := fileIndex.All()
				var found []domain.FileInfo

				for _, f := range allFiles {
					for _, pid := range fileIndex.GetPeers(f.Checksum) {
						if pid == targetID {
							found = append(found, f)
							break
						}
					}
				}

				if len(found) == 0 {
					fmt.Printf("  Nenhum arquivo conhecido para o peer '%s'.\n", targetID)
					continue
				}

				fmt.Printf("\n  Arquivos de '%s':\n", targetID)
				fmt.Printf("  %-25s %-10s %-30s\n", "NOME", "TAMANHO", "CHECKSUM")
				fmt.Println("  " + strings.Repeat("-", 70))
				for _, f := range found {
					fmt.Printf("  %-25s %-10d %s...\n", f.Name, f.Size, f.Checksum[:16])
				}

			case "search":
				if len(args) == 0 {
					fmt.Println("  Uso: search <nome_do_arquivo>")
					continue
				}
				query := strings.Join(args, " ")
				localAddr := client.Address(self.Host, self.Port)

				resultados, err := grpcClient.SearchFiles(localAddr, query)
				if err != nil {
					fmt.Printf("  Erro na busca: %v\n", err)
					continue
				}

				if len(resultados) == 0 {
					fmt.Printf("  Nenhum arquivo encontrado para '%s'.\n", query)
					continue
				}

				fmt.Printf("\n  %-25s %-10s %-20s\n", "NOME", "TAMANHO", "PEERS (ID)")
				fmt.Println("  " + strings.Repeat("-", 60))
				for _, r := range resultados {
					peerIDs := []string{}
					for _, p := range r.Peers {
						peerIDs = append(peerIDs, p.ID)
					}
					fmt.Printf("  %-25s %-10d %v\n", r.Name, r.Size, peerIDs)
				}

			case "download":
				if len(args) < 2 {
					fmt.Println("  Uso: download <host:porta> <nome_do_arquivo>")
					continue
				}
				target := args[0]
				filename := strings.Join(args[1:], " ")

				destDir := filepath.Join("downloads", self.ID)

				fmt.Printf("  Iniciando download de '%s' vindo de %s...\n", filename, target)

				err := grpcClient.DownloadFile(target, filename, destDir, "")
				if err != nil {
					fmt.Printf("  Falha no download: %v\n", err)
				} else {
					fmt.Printf("  Download concluído! Salvo na pasta '%s'.\n", destDir)
				}

			case "exit", "quit":
				fmt.Println("  Saindo da rede...")
				os.Exit(0)

			default:
				fmt.Printf("  Comando desconhecido: '%s'. Digite 'help'.\n", cmd)
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
