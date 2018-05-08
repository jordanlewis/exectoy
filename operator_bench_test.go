package exectoy

import (
	"math/rand"
	"testing"
)

func randomizeSource(s *repeatableBatchSource) {
	seed := int64(12345)
	rngesus := rand.New(rand.NewSource(seed))

	for i := 0; i < s.numOutputCols*batchRowLen; i++ {
		s.internalBatch[i/batchRowLen].(intColumn)[i%batchRowLen] = rngesus.Int() % 128
	}
}

func randomizeTupleBatchSouce(s *repeatableTupleBatchSource) {
	seed := int64(12345)
	rngesus := rand.New(rand.NewSource(seed))

	for i := 0; i < s.numOutputCols*batchRowLen; i++ {
		s.internalBatch[i/s.numOutputCols][i%s.numOutputCols] = rngesus.Int() % 128
	}
}

func BenchmarkZeroOp(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	zeroOp := &zeroIntOp{
		input:  &source,
		colIdx: 0,
	}

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		zeroOp.Next()
	}
}

func BenchmarkFilterIntLessThanConstOperator(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var fop selLTIntIntConstOp
	fop.input = &source
	fop.constArg = 64
	fop.col1Idx = 3
	fop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		fop.Next()
	}
}

func BenchmarkProjPlusIntIntConst(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop projPlusIntIntConstOp
	rop.input = &source
	rop.colIdx = 2
	rop.constArg = 5
	rop.outputIdx = 3
	rop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop.Next()
	}
}

func BenchmarkSlice(b *testing.B) {
	bat := make([]int, 2048)
	for i := range bat {
		bat[i] = i
	}
	cols := make([][]int, 2)
	cols[0] = bat[:1024]
	cols[1] = bat[1024:]

	b.SetBytes(8 * 2048)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1024; j++ {
			cols[1][j] = cols[0][j] + 1
		}
	}
}

type fixedIntCol [1024]int

func BenchmarkArray(b *testing.B) {
	cols := make([]column, 5)

	for i := range cols {
		var col fixedIntCol
		cols[i] = &col
		for j := range col {
			col[j] = i + j
		}
	}

	colA := cols[1].(*fixedIntCol)
	colB := cols[3].(*fixedIntCol)

	b.SetBytes(8 * 2048)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1024; j++ {
			colB[j] = colA[j] + 1
		}
	}
}

func BenchmarkProjPlusIntInt(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop projPlusIntIntOp
	rop.input = &source
	rop.col1Idx = 2
	rop.col2Idx = 3
	rop.outputIdx = 3
	rop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop.Next()
	}
}

func BenchmarkRenderChain(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop projPlusIntIntOp
	rop.input = &source
	rop.col1Idx = 2
	rop.col2Idx = 3
	rop.outputIdx = 3
	rop.Init()

	var rop2 projPlusIntIntOp
	rop2.input = &rop
	rop2.col1Idx = 2
	rop2.col2Idx = 3
	rop2.outputIdx = 3
	rop2.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop2.Next()
	}
}

func BenchmarkSelectIntPlusConstLTInt(b *testing.B) {
	// this benchmarks a query like:
	// SELECT o FROM t WHERE n + 1 > m
	// on a table t [n, m, o, p]

	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	// first project n -> n+1
	projOp := projPlusIntIntConstOp{
		input:     &source,
		colIdx:    0,
		constArg:  1,
		outputIdx: 0,
	}
	projOp.Init()

	// then select (n+1) > m
	selOp := selLTIntIntOp{
		input:   &projOp,
		col1Idx: 1,
		col2Idx: 0,
	}
	selOp.Init()

	b.SetBytes(int64(8 * source.numOutputCols * batchRowLen))
	for i := 0; i < b.N; i++ {
		selOp.Next()
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

func BenchmarkSortedDistinct(b *testing.B) {
	source := &repeatableBatchSource{
		numOutputCols: 5,
	}
	source.Init()
	randomizeSource(source)

	zeroOp := &zeroIntOp{
		input:  source,
		colIdx: 4,
	}
	zeroOp.Init()

	sdop := &sortedDistinctIntOp{
		sortedDistinctCol: 1,
		outputColIdx:      4,
		input:             zeroOp,
	}
	sdop.Init()

	sdop = &sortedDistinctIntOp{
		sortedDistinctCol: 2,
		outputColIdx:      4,
		input:             sdop,
	}
	sdop.Init()

	// don't count the artificial zeroOp'd column in the throughput
	b.SetBytes(int64(8 * batchRowLen * (source.numOutputCols - 1)))
	for i := 0; i < b.N; i++ {
		sdop.Next()
	}
}

func BenchmarkMergeJoin(b *testing.B) {
	sourceL := &repeatableBatchSource{
		numOutputCols: 4,
	}
	sourceL.Init()
	randomizeSource(sourceL)

	sourceR := &repeatableBatchSource{
		numOutputCols: 4,
	}
	sourceR.Init()
	randomizeSource(sourceR)

	mj := &mergeJoinIntIntOp{
		left:          sourceL,
		right:         sourceR,
		leftEqColIdx:  1,
		rightEqColIdx: 1,
		leftCols:      []int{1, 2},
		rightCols:     []int{2, 3},
	}
	mj.Init()

	b.SetBytes(int64(8 * batchRowLen * 4))
	for i := 0; i < b.N; i++ {
		mj.Next()
	}
}
