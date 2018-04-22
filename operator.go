package exectoy

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

const batchRowLen = 1024

type column []int
type batch []column
type dataFlow struct {
	// length of batch or sel in tuples
	n      int
	b      batch
	useSel bool
	// if useSel is true, a selection vector from upstream.
	sel column
}
type tuple []int

type repeatableBatchSource struct {
	numOutputCols int
	internalBatch batch
	internalSel   column
}

var repeatableRowSourceIntSet util.FastIntSet

func (s *repeatableBatchSource) Next() dataFlow {
	return dataFlow{
		b:      s.internalBatch,
		sel:    s.internalSel,
		useSel: false,
		n:      batchRowLen,
	}
}

func (s *repeatableBatchSource) Init() {
	b := make([]int, s.numOutputCols*batchRowLen)
	s.internalBatch = make(batch, s.numOutputCols)
	s.internalSel = make(column, batchRowLen)
	for i := range s.internalBatch {
		s.internalBatch[i] = column(b[i*batchRowLen : (i+1)*batchRowLen])
	}
	repeatableRowSourceIntSet.AddRange(0, batchRowLen)
}

var _ ExecOp = &repeatableBatchSource{}

type ExecOp interface {
	Init()
	Next() dataFlow
}

/*

type copyOperator struct {
	input ExecOp

	numOutputCols int
	internalBatch batch
}

func (p *copyOperator) Init() {
	p.internalBatch = make(batch, p.numOutputCols)
}

func (p copyOperator) Next() dataFlow {
	dataFlow := p.input.Next()
	copy(p.internalBatch, b)
	return p.internalBatch, inputBitmap
}
*/
