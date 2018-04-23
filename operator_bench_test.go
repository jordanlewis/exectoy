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

func BenchmarkFilterIntLessThanConstOperator(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var fop selectLTIntIntConstOp
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

	var rop projPlusIntIntConst
	rop.input = &source
	rop.intIdx = 2
	rop.constArg = 5
	rop.outputIdx = 3
	rop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop.Next()
	}
}

func BenchmarkProjPlusIntInt(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop projPlusIntInt
	rop.input = &source
	rop.int1Idx = 2
	rop.int2Idx = 3
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

	var rop projPlusIntInt
	rop.input = &source
	rop.int1Idx = 2
	rop.int2Idx = 3
	rop.outputIdx = 3
	rop.Init()

	var rop2 projPlusIntInt
	rop2.input = &rop
	rop2.int1Idx = 2
	rop2.int2Idx = 3
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
	projOp := projPlusIntIntConst{
		input:     &source,
		intIdx:    0,
		constArg:  1,
		outputIdx: 0,
	}
	projOp.Init()

	// then select (n+1) > m
	selOp := selectLTIntIntOp{
		input:   &projOp,
		col1Idx: 1,
		col2Idx: 0,
	}
	selOp.Init()

	matOp := materializeOp{
		input: &selOp,
		cols:  []int{2},
	}
	matOp.Init()

	b.SetBytes(int64(8 * source.numOutputCols))
	for i := 0; i < b.N; i++ {
		matOp.NextRow()
	}
}

func BenchmarkSortedDistinct(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var sdop sortedDistinctOperator
	sdop.sortedDistinctCols = []int{1, 2}
	sdop.input = &source
	sdop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))
	for i := 0; i < b.N; i++ {
		sdop.Next()
	}
}
