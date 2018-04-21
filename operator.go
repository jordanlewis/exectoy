package exectoy

import (
	"github.com/cockroachdb/cockroach/pkg/util"
)

const batchRowLen = 2000

type column []int
type batch []column
type tuple []int

type repeatableBatchSource struct {
	numOutputCols int
	internalBatch batch
}

var repeatableRowSourceIntSet util.FastIntSet

func (s *repeatableBatchSource) Next() (batch, util.FastIntSet) {
	return s.internalBatch, repeatableRowSourceIntSet
}

func (s *repeatableBatchSource) Init() {
	b := make([]int, s.numOutputCols*batchRowLen)
	s.internalBatch = make(batch, s.numOutputCols)
	for i := range s.internalBatch {
		s.internalBatch[i] = column(b[i*batchRowLen : (i+1)*batchRowLen])
	}
	repeatableRowSourceIntSet.AddRange(0, batchRowLen)
}

var _ BatchRowSource = &repeatableBatchSource{}

type BatchRowSource interface {
	Init()
	Next() (batch, util.FastIntSet)
}

type filterIntLessThanConstOperator struct {
	input BatchRowSource

	colIdx   int
	constArg int

	numCols int
}

var _ BatchRowSource = &filterIntLessThanConstOperator{}

func (p *filterIntLessThanConstOperator) Init() {}

func (p *filterIntLessThanConstOperator) Next() (batch, util.FastIntSet) {
	var b batch
	// outputBitmap contains row indexes that we will output
	var outputBitmap util.FastIntSet

	for outputBitmap.Empty() {
		b, _ := p.input.Next()
		if b == nil {
			return nil, outputBitmap
		}
		col := b[p.colIdx]

		for i := 0; i < batchRowLen; i++ {
			// Filter step.
			if col[i] < p.constArg {
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
	p.internalBatch = make(batch, len(p.projections))
}

func (p projectOperator) Next() (batch, util.FastIntSet) {
	b, inputBitmap := p.input.Next()

	for i, c := range p.projections {
		p.internalBatch[i] = b[c]
	}
	return p.internalBatch, inputBitmap
}

// These will get templated implementations!
type renderIntPlusConstOperator struct {
	input BatchRowSource

	intIdx   int
	constArg int

	outputIdx     int
	numOutputCols int
}

func (p *renderIntPlusConstOperator) Next() (batch, util.FastIntSet) {
	b, inputBitmap := p.input.Next()

	renderCol := b[p.outputIdx]
	intCol := b[p.intIdx]
	for i := 0; i < batchRowLen; i++ {
		renderCol[i] = intCol[i] + p.constArg
	}
	return b, inputBitmap
}

func (p renderIntPlusConstOperator) Init() {}

type renderIntPlusIntOperator struct {
	input BatchRowSource

	int1Idx int
	int2Idx int

	outputIdx     int
	numOutputCols int
}

func (p renderIntPlusIntOperator) Next() (batch, util.FastIntSet) {
	b, inputBitmap := p.input.Next()

	renderCol := b[p.outputIdx]
	col1 := b[p.int1Idx]
	col2 := b[p.int2Idx]
	for i := 0; i < batchRowLen; i++ {
		renderCol[i] = col1[i] + col2[i]
	}
	return b, inputBitmap
}

func (p *renderIntPlusIntOperator) Init() {}

type renderIntEqualsIntOperator struct {
	input BatchRowSource

	int1Idx int
	int2Idx int
}

func (p *renderIntEqualsIntOperator) Init() {}

type sortedDistinctOperator struct {
	input BatchRowSource

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

func (p *sortedDistinctOperator) Next() (batch, util.FastIntSet) {
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
	input BatchRowSource

	numOutputCols int
	internalBatch batch
}

func (p *copyOperator) Init() {
	p.internalBatch = make(batch, p.numOutputCols)
}

func (p copyOperator) Next() (batch, util.FastIntSet) {
	b, inputBitmap := p.input.Next()
	copy(p.internalBatch, b)
	return p.internalBatch, inputBitmap
}
