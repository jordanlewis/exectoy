package exectoy

const batchRowLen = 1024

type column interface{}
type batch []column
type tuple []int

type intColumn []int
type float64Column []float64
type boolColumn []bool

// dataFlow is the batch format passed around by operators.
type dataFlow struct {
	// length of batch or sel in tuples
	n int
	// slice of columns in this batch.
	b      batch
	useSel bool
	// if useSel is true, a selection vector from upstream. a selection vector is
	// a list of selected column indexes in this dataFlow's columns.
	sel intColumn
}

// ExecOp is an exectoy operator.
type ExecOp interface {
	Init()
	Next() dataFlow
}

// TupleSource returns a tuple on each call to NextTuple.
type TupleSource interface {
	NextTuple() tuple
}

type repeatableBatchSource struct {
	numOutputCols int
	internalBatch batch
	internalSel   intColumn
}

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
	s.internalSel = make(intColumn, batchRowLen)
	for i := range s.internalBatch {
		s.internalBatch[i] = intColumn(b[i*batchRowLen : (i+1)*batchRowLen])
	}
}

var _ ExecOp = &repeatableBatchSource{}

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
