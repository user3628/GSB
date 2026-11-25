package convergence

import "crypto/rand"

func readRandom(data []byte) (int, error) { return rand.Read(data) }
