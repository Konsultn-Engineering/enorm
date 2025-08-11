package ast

import (
	"fmt"
	"github.com/Konsultn-Engineering/enorm/utils"
	"strconv"
)

type ValueType int

const (
	ValueNull ValueType = iota
	ValueBool
	ValueInt
	ValueFloat
	ValueString
	ValueTime
)

type Value struct {
	Val       interface{}
	ValueType ValueType
}

func NewValue(val any) *Value {
	v := valuePool.Get().(*Value)
	v.Val = val
	return v
}

func (v *Value) Type() NodeType           { return NodeValue }
func (v *Value) Accept(vis Visitor) error { return vis.VisitValue(v) }
func (v *Value) Fingerprint() uint64 {
	s := "val:" + strconv.FormatInt(int64(v.Type()), 10) + ":" + fmt.Sprint(v.Val)
	return utils.FingerprintString(s)
}

func (v *Value) Release() {
	valuePool.Put(v)
}
