package cache

import (
	"encoding/binary"
)

type Method uint8

const (
	MethodFind Method = iota
	MethodFindAll
	MethodFindOne
	MethodCount
	MethodExists
	MethodSum
	MethodAvg
)

type FixedKey [32]byte

func GenerateFixedKey(method Method, entityID uint32) FixedKey {
	var key FixedKey

	// Layout:
	// [0]:     Method (1 byte)
	// [1-4]:   Entity ID (4 bytes)
	// [5-31]: Reserved/Additional params

	key[0] = byte(method)
	binary.BigEndian.PutUint32(key[1:5], entityID)

	return key // No heap allocation - returns by value
}
