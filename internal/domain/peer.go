package domain

type Peer struct {
	ID        string
	Host      string
	Port      int
	LastSeen  int64
	Heartbeat int64
	Alive     bool
}
