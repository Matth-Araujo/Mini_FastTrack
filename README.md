# 🚀 Mini FastTrack (Go P2P File Sharing)

Um sistema de compartilhamento de arquivos 100% descentralizado (Peer-to-Peer) desenvolvido em **Go** utilizando **gRPC** e o protocolo **Gossip**. 

Este projeto foi construído para ser altamente resiliente e não depende de nenhum "Super Nó" ou servidor central de descoberta, cumprindo todos os requisitos de um sistema P2P puro.

---

## 🎯 Como o projeto atende aos Requisitos da Disciplina

| Requisito | Como foi resolvido na Arquitetura |
| :--- | :--- |
| **1. Descoberta e Registro** | Qualquer nó pode atuar como *bootstrap*. Novos nós enviam um `RegisterPeer` e a rede fofoca (Gossip) a chegada do novo integrante. |
| **2. Sem Servidor Central** | **Arquitetura Leaderless.** Não há nó líder. Se o ponto de entrada original cair, a rede continua intacta e operante. |
| **3. Lista de Peers Ativos** | Mantida localmente em cada nó através de *Heartbeats* periódicos. Nós inativos são varridos automaticamente (após 30 segundos). |
| **4. Lista de Arquivos** | Os metadados dos arquivos (nome, tamanho, checksum) são indexados e sincronizados em background na memória de cada nó. |
| **5. Download Direto (P2P)** | Ocorre diretamente de par para par (`Peer -> Peer`) via *gRPC Streaming*, dividindo o arquivo em *chunks* de 32KB. |
| **6. Protocolo de Rede** | Implementado protocolo customizado sobre **gRPC / Protocol Buffers** (TCP). **Nenhuma biblioteca P2P pronta foi utilizada.** |
| **🌟 Diferencial (Extra)** | **Validação de Integridade (Checksum SHA-256).** Se um arquivo for baixado corrompido, o sistema detecta, descarta e avisa o usuário. |

---

## 🛠️ Tecnologias Utilizadas
* **Linguagem:** Go (Golang) 
* **Comunicação RPC:** gRPC & Protocol Buffers (`protoc`)
* **Consenso/Descoberta:** Algoritmo Epidêmico (Gossip Protocol)
* **Hashing:** Criptografia SHA-256 para integridade de arquivos

---

## ⚙️ Estrutura do Projeto

* `cmd/peer/main.go`: Ponto de entrada do Nó/Servidor. Mantém a rede viva e disponibiliza um terminal interativo.
* `cmd/client/`: Ferramenta CLI estática alternativa para disparar comandos isolados.
* `internal/discovery/`: Lógica do *Gossip Protocol*, Tabela de Roteamento (*PeerTable*) e Índice de Arquivos (*FileIndex*).
* `internal/server/` & `internal/client/`: Implementação dos contratos gRPC.
* `proto/`: Contratos e definições de mensagens do Protocol Buffers (`p2p.proto`).

---

## 🚀 Como Executar a Rede

Para simular o sistema, você precisará de múltiplos terminais (simulando computadores diferentes). Os arquivos compartilhados por um nó devem ficar em uma pasta `./files/<PeerID>/` criada automaticamente na raiz.

### 1. Subindo o "Nó Semente" (Bootstrap)
O primeiro nó a entrar na rede não conhece ninguém. Ele apenas abre sua porta e começa a escutar.
```bash
go run ./cmd/peer/main.go p1 127.0.0.1 5001
```

### 2. Conectando novos Peers à rede
Para ligar o `p2` e o `p3`, passamos o endereço de algum nó que já está lá dentro (neste caso, o `p1` ou até mesmo o `p2`).

```bash
# Terminal 2 (Conecta no p1)
go run ./cmd/peer/main.go p2 127.0.0.1 5002 127.0.0.1:5001

# Terminal 3 (Conecta no p2, descobrindo o p1 via fofoca)
go run ./cmd/peer/main.go p3 127.0.0.1 5003 127.0.0.1:5002
```

---

## 💻 Interface Interativa (CLI)

Após iniciar um peer, ele abrirá um terminal interativo (`fasttrack>`) para você realizar operações na rede P2P.
Os comandos disponíveis são:

* **`help`** : Lista todos os comandos disponíveis.
* **`peers`** : Mostra a tabela de nós ativos conhecidos por este peer, com IP e Porta.
* **`myfiles`** : Lista os arquivos que estão na sua própria pasta local (ex: `./files/p1/`).
* **`files <peer_id>`** : Consulta no índice global os arquivos disponibilizados por um usuário específico (ex: `files p2`).
* **`search <nome_do_arquivo>`** : Busca na rede inteira por arquivos que contenham o termo digitado e mostra quais peers possuem esse arquivo.
* **`download <host:porta> <nome_do_arquivo>`** : Conecta diretamente ao peer fornecedor e baixa o arquivo para a sua pasta `./downloads/<PeerID>/`.
* **`exit`** ou **`quit`** : Desconecta o nó e encerra o sistema.

---

## 🛡️ Tratamento de Erros e Resiliência

* **Desconexões Abruptas:** Se um nó for finalizado repentinamente (`Ctrl+C` ou queda de energia), o nó falhará em enviar *Heartbeats*. O sistema detecta a ausência em 15 segundos e remove o nó definitivamente da tabela em 30 segundos, limpando seus arquivos do índice de buscas.
* **Tratamento de Arquivos:** Se o arquivo não existir no destino ou o download falhar no meio da transferência, o gRPC cancela a operação e o terminal exibe o erro tratável sem derrubar o processo.
* **Bootstrap Redundante:** O código suporta múltiplos IPs de inicialização. É possível passar `IP:PORTA,IP:PORTA` no terminal. Se o primeiro ponto falhar, ele tenta o próximo contato para entrar na rede.