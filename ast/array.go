package ast

import (
	"fmt"
	"hash/fnv"
	"strconv"
)

type Array struct {
	Values []Value
}

func (a *Array) Type() NodeType {
	return NodeArray
}

func (a *Array) Accept(v Visitor) error {
	return v.VisitArray(a)
}

func (a *Array) Fingerprint() uint64 {
	h := fnv.New64()
	h.Write([]byte("array:"))
	for _, val := range a.Values {
		h.Write([]byte(strconv.Itoa(int(val.ValueType))))
		h.Write([]byte(fmt.Sprintf("%v,", val.Val)))
	}
	return h.Sum64()
}
