# Mini FastTrack

Sistema P2P em Go com descoberta de peers via gossip, bootstrap via gRPC e transferência de arquivos por streaming.

## Funcionalidades

- Descoberta de peers com bootstrap.
- Gossip com heartbeat, detecção de falha e remoção de peers mortos.
- Listagem de arquivos disponíveis em cada peer.
- Download de arquivos via stream gRPC.
- Testes automatizados para discovery e server.

## Estrutura do projeto

- `cmd/peer`: executa um peer completo.
- `cmd/client`: cliente simples para listar e baixar arquivos.
- `internal/client`: cliente gRPC.
- `internal/discovery`: tabela de peers e gossip.
- `internal/domain`: tipos de domínio.
- `internal/server`: servidor gRPC.
- `proto`: arquivos `.proto` e código gerado.

## Requisitos

- Go 1.22+
- `protoc`
- plugins do Go para protobuf:
  - `protoc-gen-go`
  - `protoc-gen-go-grpc`

## Como gerar os arquivos protobuf

Se alterar o `.proto`, regenere com:

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/p2p.proto
```

## Como executar um peer

Exemplo de execução:

```bash
go run ./cmd/peer peer1 127.0.0.1 5001
```

Com bootstrap:

```bash
go run ./cmd/peer peer2 127.0.0.1 5002 127.0.0.1:5001
go run ./cmd/peer peer3 127.0.0.1 5003 127.0.0.1:5001
```

### Parâmetros

- `peerID`: identificador do peer.
- `host`: endereço local.
- `port`: porta do peer.
- `bootstrapHost:port`: um ou mais peers bootstrap separados por vírgula.

Exemplo com múltiplos bootstraps:

```bash
go run ./cmd/peer peer4 127.0.0.1 5004 127.0.0.1:5001,127.0.0.1:5002
```

## Como usar o cliente

### Listar arquivos

```bash
go run ./cmd/client 127.0.0.1:5001 list
```

### Baixar arquivo

```bash
go run ./cmd/client 127.0.0.1:5001 download hello.txt
```

O arquivo será salvo em:

```bash
./downloads/hello.txt
```

## Pastas de arquivos

Cada peer serve arquivos da pasta:

```bash
files/<peerID>/
```

Exemplo:

```bash
mkdir -p files/peer1
echo "hello from peer1" > files/peer1/hello.txt
```

## Testes

Executar todos os testes:

```bash
go test ./...
```

Testar apenas discovery:

```bash
go test ./internal/discovery -v
```

Testar apenas server:

```bash
go test ./internal/server -v
```

## Fluxo de funcionamento

1. O peer sobe o servidor gRPC.
2. Se houver bootstrap, ele registra o peer no bootstrap.
3. O bootstrap devolve seu próprio `self` e os peers conhecidos.
4. O gossip começa a trocar estado com outros peers.
5. O servidor expõe os arquivos locais via `ListFiles` e `DownloadFile`.

## Observações

- O gossip usa heartbeat para atualização de estado.
- Peers que param de responder são marcados como mortos.
- Depois de um tempo, peers mortos são removidos da tabela.

## Exemplo de teste manual

Terminal 1:

```bash
go run ./cmd/peer peer1 127.0.0.1 5001
```

Terminal 2:

```bash
go run ./cmd/peer peer2 127.0.0.1 5002 127.0.0.1:5001
```

Terminal 3:

```bash
go run ./cmd/peer peer3 127.0.0.1 5003 127.0.0.1:5001
```

Depois:

```bash
go run ./cmd/client 127.0.0.1:5001 list
go run ./cmd/client 127.0.0.1:5001 download hello.txt
```
