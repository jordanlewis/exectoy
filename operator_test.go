package exectoy

import (
	"reflect"
	"testing"
)

// genTuples makes n tuples with c cols
func genTuples(n int, c int) []tuple {
	ret := make([]tuple, n)
	for i := 0; i < n; i++ {
		ret[i] = make(tuple, c)
		for j := 0; j < c; j++ {
			ret[i][j] = i*c + j
		}
	}
	return ret
}

func TestColumnarizeMaterialize(t *testing.T) {
	tuples := genTuples(10, 3)
	tupleSource := &staticTupleSource{
		tuples: tuples,
	}
	columnarizeOp := &columnarizeOp{
		input:   tupleSource,
		numCols: 3,
	}
	columnarizeOp.Init()
	materializeOp := &materializeOp{
		input: columnarizeOp,
		cols:  []int{0, 1, 2},
	}
	materializeOp.Init()

	for i := 0; i < 10; i++ {
		tuple := materializeOp.NextTuple()
		if !reflect.DeepEqual(tuple, tuples[i]) {
			t.Errorf("expected %v, got %v", tuples[i], tuple)
		}
	}
}

func TestSortedDistinct(t *testing.T) {
	tcs := []struct {
		distinctCols []int
		numCols      int
		tuples       []tuple
		expected     []tuple
	}{
		{
			distinctCols: []int{0, 1, 2},
			numCols:      4,
			tuples: []tuple{
				tuple{1, 2, 3, 4},
				tuple{1, 2, 3, 5},
				tuple{2, 2, 3, 4},
				tuple{2, 3, 3, 4},
				tuple{2, 3, 4, 4},
				tuple{2, 3, 4, 4},
			},
			expected: []tuple{
				tuple{1, 2, 3, 4},
				tuple{2, 2, 3, 4},
				tuple{2, 3, 3, 4},
				tuple{2, 3, 4, 4},
			},
		},
	}

	for _, tc := range tcs {
		tupleSource := &staticTupleSource{
			tuples: tc.tuples,
		}

		columnarizeOp := &columnarizeOp{
			input:   tupleSource,
			numCols: tc.numCols + 1,
		}
		columnarizeOp.Init()

		zeroOp := &zeroIntOp{
			input:  columnarizeOp,
			colIdx: tc.numCols,
		}
		zeroOp.Init()

		var lastOp ExecOp
		lastOp = zeroOp
		for _, cIdx := range tc.distinctCols {
			sdop := &sortedDistinctIntOp{
				input:             lastOp,
				sortedDistinctCol: cIdx,
				outputColIdx:      tc.numCols,
			}
			sdop.Init()
			lastOp = sdop
		}

		finalizer := &sortedDistinctFinalizerOp{
			input:        lastOp,
			outputColIdx: tc.numCols,
		}

		mop := &materializeOp{
			input: finalizer,
			cols:  []int{0, 1, 2, 3},
		}
		mop.Init()

		var actual []tuple
		for {
			tuple := mop.NextTuple()
			if tuple == nil {
				break
			}
			actual = append(actual, tuple)
		}
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("expected %v, got %v", tc.expected, actual)
		}
	}
}

func TestMergeJoin(t *testing.T) {
	tcs := []struct {
		leftEqColIdx  int
		rightEqColIdx int
		leftNCols     int
		rightNCols    int
		leftCols      []int
		rightCols     []int
		leftTuples    []tuple
		rightTuples   []tuple
		expected      []tuple
	}{
		{
			leftEqColIdx:  0,
			rightEqColIdx: 1,
			leftNCols:     4,
			rightNCols:    4,
			leftTuples: []tuple{
				tuple{1, 2, 3, 4},
				tuple{5, 2, 3, 5},
			},
			rightTuples: []tuple{
				tuple{1, 5, 3, 4},
				tuple{1, 6, 3, 5},
			},
			leftCols:  []int{0, 1},
			rightCols: []int{1, 2},
			expected: []tuple{
				tuple{5, 2, 5, 3},
			},
		},
	}

	for _, tc := range tcs {
		leftSource := &staticTupleSource{
			tuples: tc.leftTuples,
		}

		lColOp := &columnarizeOp{
			input:   leftSource,
			numCols: tc.leftNCols,
		}
		lColOp.Init()

		rightSource := &staticTupleSource{
			tuples: tc.rightTuples,
		}
		rColOp := &columnarizeOp{
			input:   rightSource,
			numCols: tc.rightNCols,
		}
		rColOp.Init()

		mj := &mergeJoinIntIntOp{
			left:          lColOp,
			right:         rColOp,
			leftEqColIdx:  tc.leftEqColIdx,
			rightEqColIdx: tc.rightEqColIdx,
			leftCols:      tc.leftCols,
			rightCols:     tc.rightCols,
		}
		mj.Init()

		mop := &materializeOp{
			input: mj,
			cols:  []int{0, 1, 2, 3},
		}
		mop.Init()

		var actual []tuple
		for {
			tuple := mop.NextTuple()
			if tuple == nil {
				break
			}
			actual = append(actual, tuple)
		}
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("expected %v, got %v", tc.expected, actual)
		}
	}
}
