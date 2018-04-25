package exectoy

import "testing"

// TupleBatchSource returns a batch of tuples on each call to NextTuple.
type TupleBatchSource interface {
	NextTupleBatch() []tuple
}

type repeatableTupleSource struct {
	t tuple
}

var _ TupleSource = &repeatableTupleSource{}

func (s *repeatableTupleSource) NextTuple() tuple {
	return s.t
}

type repeatableTupleBatchSource struct {
	numOutputCols int
	internalBatch []tuple
}

var _ TupleBatchSource = &repeatableTupleBatchSource{}

func (s *repeatableTupleBatchSource) Init() {
	b := make([]int, s.numOutputCols*batchRowLen)
	s.internalBatch = make([]tuple, batchRowLen)
	for i := range s.internalBatch {
		s.internalBatch[i] = tuple(b[i*s.numOutputCols : (i+1)*s.numOutputCols])
	}
}

func (s *repeatableTupleBatchSource) NextTupleBatch() []tuple {
	return s.internalBatch
}

// this is a copy of BenchmarkSelectIntPlusConstLTInt that uses a per-tuple
// interface instead of a columnarized one. It's also super efficient because it
// doesn't even have to call any kind of Expr.Eval thing.
type rowBasedFilterIntLessThanConst struct {
	input         TupleSource
	internalTuple tuple
}

func (r rowBasedFilterIntLessThanConst) NextTuple() tuple {
	for {
		t := r.input.NextTuple()
		if t[0]+1 > t[1] {
			r.internalTuple[0] = t[2]
			return r.internalTuple
		}
	}
}

func BenchmarkRowBasedFilterIntLessThanConst(b *testing.B) {
	// this benchmarks a query like:
	// SELECT o FROM t WHERE n + 1 > m
	// on a table t [n, m, o, p]
	source := &repeatableTupleSource{
		t: []int{2, 2, 3, 4},
	}
	f := &rowBasedFilterIntLessThanConst{
		input:         source,
		internalTuple: make(tuple, 1),
	}
	b.SetBytes(int64(8 * 4))
	for i := 0; i < b.N; i++ {
		f.NextTuple()
	}
}

type rowBatchBasedFilterIntLessThanConst struct {
	input       TupleBatchSource
	internalSel column
}

func (r *rowBatchBasedFilterIntLessThanConst) Init() {
	r.internalSel = make(column, batchRowLen)
}

func (r *rowBatchBasedFilterIntLessThanConst) NextTupleBatch() []tuple {
	t := r.input.NextTupleBatch()
	idx := 0
	for i := range t {
		if t[i][0]+1 > t[i][1] {
			r.internalSel[i] = idx
			idx++
		}
	}
	return t
}

func BenchmarkRowBatchBasedFilterIntLessThanConst(b *testing.B) {
	// this benchmarks a query like:
	// SELECT o FROM t WHERE n + 1 > m
	// on a table t [n, m, o, p]
	source := &repeatableTupleBatchSource{
		numOutputCols: 4,
	}
	source.Init()
	randomizeTupleBatchSouce(source)
	f := &rowBatchBasedFilterIntLessThanConst{
		input: source,
	}
	f.Init()
	b.SetBytes(int64(8 * 4 * batchRowLen))
	for i := 0; i < b.N; i++ {
		f.NextTupleBatch()
	}
}
