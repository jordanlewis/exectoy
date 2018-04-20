package distsqlrun

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
