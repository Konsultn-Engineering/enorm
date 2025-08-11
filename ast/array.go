package ast

import (
	"fmt"
	"hash/fnv"
	"strconv"
)

type Array struct {
	Values []Value
}

func NewArray(values []any) *Array {
	a := arrayPool.Get().(*Array)
	a.Values = a.Values[:0]

	for _, val := range values {
		a.Values = append(a.Values, Value{Val: val})
	}
	return a
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

func (a *Array) Release() {
	a.Values = a.Values[:0]
	arrayPool.Put(a)
}
