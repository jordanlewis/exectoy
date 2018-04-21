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
type sortedDistinctOperator struct {
	input ExecOp

	sortedDistinctCols []int
	cols               []column

	lastVal tuple
	curVal  tuple

	numOutputCols int
	internalBatch batch
}

func (p *sortedDistinctOperator) Init() {
	p.internalBatch = make(batch, p.numOutputCols)
	p.cols = make([]column, len(p.sortedDistinctCols))
	p.lastVal = make(tuple, len(p.sortedDistinctCols))
	p.curVal = make(tuple, len(p.sortedDistinctCols))
}

func (p *sortedDistinctOperator) Next() dataFlow {
	// outputBitmap contains row indexes that we will output
	var b batch
	var outputBitmap util.FastIntSet
	for outputBitmap.Empty() {
		b, _ = p.input.Next()

		for i, c := range p.sortedDistinctCols {
			p.cols[i] = b[c]
		}

		for r := 0; r < batchRowLen; r++ {
			emit := false
			for i := range p.sortedDistinctCols {
				col := p.cols[i][r]
				p.curVal[i] = col
				if col != p.lastVal[i] {
					emit = true
				}
			}
			if emit {
				copy(p.lastVal, p.curVal)
				outputBitmap.Add(r)
			}
		}
	}
	return b, outputBitmap
}

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
