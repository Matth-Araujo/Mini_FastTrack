package domain

type FileEntry struct {
	Name     string
	Size     int64
	Checksum string
	Peers    []string
}
