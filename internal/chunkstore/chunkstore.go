package chunkstore

import (
	"crypto/sha256"
)

type ChunkHash = [sha256.Size]byte
