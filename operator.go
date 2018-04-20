package distsqlrun

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

const batchRowLen = 16

type batch []int

type repeatableBatchSource struct {
	numOutputCols int
	internalBatch batch
}

var repeatableRowSourceIntSet util.FastIntSet

func (s *repeatableBatchSource) Next() (batch, util.FastIntSet) {
	return s.internalBatch, repeatableRowSourceIntSet
}

func (s *repeatableBatchSource) Init() {
	s.internalBatch = make(batch, s.numOutputCols*batchRowLen)
	repeatableRowSourceIntSet.AddRange(0, batchRowLen)
}

var _ BatchRowSource = &repeatableBatchSource{}

type BatchRowSource interface {
	Init()
	Next() (batch, util.FastIntSet)
}

type filterOperator struct {
	input BatchRowSource

	numCols int
}

var _ BatchRowSource = &filterOperator{}

func (p *filterOperator) Init() {}

func (p filterOperator) Next() (batch, util.FastIntSet) {
	var b batch
	// outputBitmap contains row indexes that we will output
	var outputBitmap util.FastIntSet

	for outputBitmap.Empty() {
		b, inputBitmap := p.input.Next()
		if b == nil {
			return nil, outputBitmap
		}

		// Select b where a > 64
		bCol := 2

		for i := 0; i < batchRowLen; i++ {
			if !inputBitmap.Contains(i) {
				continue
			}
			// Filter step.
			if b[i+(batchRowLen*(bCol-1))] > 64 {
				outputBitmap.Add(i)
			}
		}
	}
	return b, outputBitmap
}

type projectOperator struct {
	input BatchRowSource

	projections []int

	internalBatch batch
}

func (p *projectOperator) Init() {
	p.internalBatch = make(batch, len(p.projections)*batchRowLen)
}

func (p projectOperator) Next() (batch, util.FastIntSet) {
	b, inputBitmap := p.input.Next()

	for i, c := range p.projections {
		copy(p.internalBatch[i*batchRowLen:i*batchRowLen+batchRowLen], b[c*batchRowLen:(c*batchRowLen)+batchRowLen])
	}
	return p.internalBatch, inputBitmap
}
