package domain

type FileInfo struct {
	Name     string
	Size     int64
	Checksum string
}

type IndexedFile struct {
	Name     string
	Size     int64
	Checksum string
	Peers    []Peer
}
