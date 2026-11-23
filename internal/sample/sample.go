package sample

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// BlockContent deterministically expands a short seed into exactly size bytes.
// The only entropy source is the supplied seed and structured block/generation
// coordinates; no wall-clock or OS randomness participates.
func BlockContent(seed string, blockNo, generation int64, size int) []byte {
	if size < 0 {
		panic("negative block size")
	}
	out := make([]byte, size)
	var counter uint64
	for offset := 0; offset < size; {
		h := sha256.New()
		h.Write([]byte(seed))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(blockNo))
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], uint64(generation))
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], counter)
		h.Write(buf[:])
		sum := h.Sum(nil)
		offset += copy(out[offset:], sum)
		counter++
	}
	return out
}

func DigestHex(content []byte) string {
	sum := sha256.Sum256(content)
	return fmt.Sprintf("%x", sum[:])
}
