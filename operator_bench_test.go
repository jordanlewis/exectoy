package exectoy

import (
	"math/rand"
	"testing"
)

func randomizeSource(s *repeatableBatchSource) {
	seed := int64(12345)
	rngesus := rand.New(rand.NewSource(seed))

	for i := 0; i < s.numOutputCols*batchRowLen; i++ {
		s.internalBatch[i/batchRowLen][i%batchRowLen] = rngesus.Int() % 128
	}
}

func randomizeTupleBatchSouce(s *repeatableTupleBatchSource) {
	seed := int64(12345)
	rngesus := rand.New(rand.NewSource(seed))

	for i := 0; i < s.numOutputCols*batchRowLen; i++ {
		s.internalBatch[i/s.numOutputCols][i%s.numOutputCols] = rngesus.Int() % 128
	}
}

func BenchmarkFilterIntLessThanConstOperator(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}
	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			&selectLTIntIntConstOp{
				constArg: 64,
				col1Idx:  3,
			},
		},
	}
	engine.Init()

	randomizeSource(source)

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		engine.Next()
	}
}

func BenchmarkProjPlusIntIntConst(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}

	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			&projPlusIntIntConst{
				intIdx:    2,
				constArg:  5,
				outputIdx: 3,
			},
		},
	}
	engine.Init()
	randomizeSource(source)

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		engine.Next()
	}
}

func BenchmarkProjPlusIntInt(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}

	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			&projPlusIntInt{
				int1Idx:   2,
				int2Idx:   3,
				outputIdx: 3,
			},
		},
	}
	engine.Init()
	randomizeSource(source)

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		engine.Next()
	}
}

func BenchmarkRenderChain(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}

	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			&projPlusIntInt{
				int1Idx:   2,
				int2Idx:   3,
				outputIdx: 3,
			},
			&projPlusIntInt{
				int1Idx:   2,
				int2Idx:   3,
				outputIdx: 3,
			},
		},
	}
	engine.Init()
	randomizeSource(source)

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		engine.Next()
	}
}

func BenchmarkSelectIntPlusConstLTInt(b *testing.B) {
	// this benchmarks a query like:
	// SELECT o FROM t WHERE n + 1 > m
	// on a table t [n, m, o, p]
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}

	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			// first project n -> n+1
			&projPlusIntIntConst{
				intIdx:    0,
				constArg:  1,
				outputIdx: 0,
			},
			// then select (n+1) > m
			&selectLTIntIntOp{
				col1Idx: 1,
				col2Idx: 0,
			},
		},
	}
	engine.Init()
	randomizeSource(source)

	b.SetBytes(int64(8 * source.numOutputCols * batchRowLen))
	for i := 0; i < b.N; i++ {
		engine.Next()
	}

	/*
		matOp := materializeOp{
			input: &selOp,
			cols:  []int{2},
		}
		matOp.Init()

		b.SetBytes(int64(8 * source.numOutputCols))
		for i := 0; i < b.N; i++ {
			matOp.NextTuple()
		}
	*/
}

// this is a copy of the test above that uses a per-tuple interface instead of a
// columnarized one. It's also super efficient because it doesn't even have to
// call any kind of Expr.Eval thing.
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

func BenchmarkSortedDistinct(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 4,
	}

	engine := Engine{
		source: source,
		pipeline: []ExecOp{
			&sortedDistinctOp{
				sortedDistinctCols: []int{1, 2},
			},
		},
	}
	engine.Init()
	randomizeSource(source)

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))
	for i := 0; i < b.N; i++ {
		engine.Next()
	}
}
