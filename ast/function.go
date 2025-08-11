package ast

import (
	"github.com/Konsultn-Engineering/enorm/utils"
	"hash/fnv"
)

type Function struct {
	Name string
	Args []Node
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
