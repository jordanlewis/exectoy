package main

import (
	"math/rand"
	"testing"
)

func randomizeSource(s *repeatableBatchSource) {
	seed := int64(12345)
	rngesus := rand.New(rand.NewSource(seed))

	for i := 0; i < s.numOutputCols*batchRowLen; i++ {
		s.internalBatch[i] = rngesus.Int() % 128
	}
}

func BenchmarkFilterOperator(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var fop filterOperator
	fop.input = &source
	fop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		fop.Next()
	}
}

func BenchmarkProjectOperator(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var pop projectOperator
	pop.input = &source
	// project 2 out of 4 columns
	pop.projections = []int{1, 2}
	pop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		pop.Next()
	}
}

func BenchmarkRenderIntPlusConst(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop renderIntPlusConstOperator
	rop.input = &source
	rop.intIdx = 2
	rop.constArg = 5
	rop.outputIdx = 3
	rop.numOutputCols = source.numOutputCols
	rop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop.Next()
	}
}

func BenchmarkRenderIntPlusInt(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var rop renderIntPlusIntOperator
	rop.input = &source
	rop.int1Idx = 2
	rop.int2Idx = 3
	rop.outputIdx = 3
	rop.numOutputCols = source.numOutputCols
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

	var rop renderIntPlusIntOperator
	rop.input = &source
	rop.int1Idx = 2
	rop.int2Idx = 3
	rop.outputIdx = 3
	rop.numOutputCols = source.numOutputCols
	rop.Init()

	var rop2 renderIntPlusIntOperator
	rop2.input = &rop
	rop2.int1Idx = 2
	rop2.int2Idx = 3
	rop2.outputIdx = 3
	rop2.numOutputCols = rop.numOutputCols
	rop2.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))

	for i := 0; i < b.N; i++ {
		rop2.Next()
	}
}

func BenchmarkSortedDistinct(b *testing.B) {
	var source repeatableBatchSource
	source.numOutputCols = 4
	source.Init()
	randomizeSource(&source)

	var sdop sortedDistinctOperator
	sdop.sortedDistinctCols = []int{1, 2}
	sdop.numOutputCols = 4
	sdop.input = &source
	sdop.Init()

	b.SetBytes(int64(8 * batchRowLen * source.numOutputCols))
	for i := 0; i < b.N; i++ {
		sdop.Next()
	}
}
