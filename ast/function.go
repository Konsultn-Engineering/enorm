package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type Function struct {
	Name string
	Args []Node
}

func NewFunction(name string, args ...Node) *Function {
	f := functionPool.Get().(*Function)
	f.Name = name
	f.Args = f.Args[:0] // Clear existing args
	for _, arg := range args {
		if arg != nil {
			f.Args = append(f.Args, arg)
		}
	}
	return f
}

func (f *Function) Type() NodeType         { return NodeFunction }
func (f *Function) Accept(v Visitor) error { return v.VisitFunction(f) }
func (f *Function) Fingerprint() uint64 {
	h := fnv.New64a()
	h.Write([]byte("func:" + f.Name))
	for _, arg := range f.Args {
		if fp, ok := arg.(Node); ok {
			h.Write(utils.U64ToBytes(fp.Fingerprint()))
		}
	}
	return h.Sum64()
}
