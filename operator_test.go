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
		engine := &Engine{
			source: &columnarizeOp{
				input:   tupleSource,
				numCols: tc.numCols,
			},
			pipeline: []ExecOp{
				&sortedDistinctOp{
					sortedDistinctCols: tc.distinctCols,
				},
			},
		}
		engine.Init()

		mop := &materializeOp{
			input: engine,
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
