package utils

import "hash/fnv"

func U64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func Mix64(a, b uint64) uint64 {
	h := fnv.New64a()
	h.Write(U64ToBytes(a))
	h.Write(U64ToBytes(b))
	return h.Sum64()
}
