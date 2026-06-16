package main

import (
	"fmt"
	"log"
	"os"

	"go-api/internal/client"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatalf("uso: go run ./cmd/client <targetHost:port> <comando> [args]")
	}

	target := os.Args[1]
	cmd := os.Args[2]

	c := client.NewP2PClient(5 * 1e9)

	switch cmd {
	case "ping":
		if err := c.Ping(target); err != nil {
			log.Fatalf("ping falhou: %v", err)
		}
		fmt.Println("pong")

	case "list":
		files, err := c.ListFiles(target)
		if err != nil {
			log.Fatalf("erro ao listar arquivos: %v", err)
		}
		fmt.Printf("arquivos em %s:\n", target)
		for _, f := range files {
			fmt.Printf("  - %s (%d bytes) checksum=%s\n", f.Name, f.Size, f.Checksum)
		}

	case "index":
		files, err := c.GetFileIndex(target)
		if err != nil {
			log.Fatalf("erro ao obter índice: %v", err)
		}

		peers, err := c.GetPeers(target)
		if err != nil {
			log.Fatalf("erro ao obter peers: %v", err)
		}

		fmt.Println("arquivos indexados:")
		if len(files) == 0 {
			fmt.Println("  (nenhum)")
		} else {
			for _, f := range files {
				fmt.Printf("  - %s (%d bytes) checksum=%s peers=", f.Name, f.Size, f.Checksum)
				for i, p := range f.Peers {
					if i > 0 {
						fmt.Print(", ")
					}
					fmt.Printf("%s(%s:%d)", p.ID, p.Host, p.Port)
				}
				fmt.Println()
			}
		}

		fmt.Println("peers conhecidos:")
		if len(peers) == 0 {
			fmt.Println("  (nenhum)")
		} else {
			for _, p := range peers {
				fmt.Printf("  - %s %s:%d heartbeat=%d alive=%v\n", p.ID, p.Host, p.Port, p.Heartbeat, p.Alive)
			}
		}

	case "download":
		if len(os.Args) < 5 {
			log.Fatalf("uso: go run ./cmd/client <targetHost:port> download <filename> <destDir>")
		}
		filename := os.Args[3]
		destDir := os.Args[4]
		if err := c.DownloadFile(target, filename, destDir, ""); err != nil {
			log.Fatalf("download falhou: %v", err)
		}
		fmt.Println("download concluído")

	case "download-checksum":
		if len(os.Args) < 5 {
			log.Fatalf("uso: go run ./cmd/client <targetHost:port> download-checksum <checksum> <destDir>")
		}
		checksum := os.Args[3]
		destDir := os.Args[4]

		files, err := c.GetFileIndex(target)
		if err != nil {
			log.Fatalf("erro ao obter índice: %v", err)
		}

		var filename string
		var peers []string
		found := false

		for _, f := range files {
			if f.Checksum == checksum {
				filename = f.Name
				for _, p := range f.Peers {
					peers = append(peers, fmt.Sprintf("%s:%d", p.Host, p.Port))
				}
				found = true
				break
			}
		}

		if !found {
			log.Fatalf("checksum não encontrado: %s", checksum)
		}

		if len(peers) == 0 {
			log.Fatalf("nenhum peer disponível para checksum: %s", checksum)
		}

		var lastErr error
		for _, addr := range peers {
			if err := c.DownloadFile(addr, filename, destDir, checksum); err == nil {
				fmt.Println("download concluído")
				return
			} else {
				lastErr = err
				log.Printf("falha ao baixar de %s: %v", addr, err)
			}
		}

		log.Fatalf("não foi possível baixar o arquivo. último erro: %v", lastErr)

	default:
		log.Fatalf("comando desconhecido: %s", cmd)
	}
}
