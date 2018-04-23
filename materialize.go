package exectoy

// materializeOp takes dataFlows and turns them into tuples.
type materializeOp struct {
	input ExecSource

	cols []int

	rowsBuf []tuple
	rows    []tuple
}

func (t *materializeOp) Init() {
	if t.cols == nil {
		panic("didn't set cols on materializeOp")
	}
	t.rowsBuf = make([]tuple, batchRowLen)
	for i := range t.rowsBuf {
		t.rowsBuf[i] = make(tuple, len(t.cols))
	}
	t.rows = t.rowsBuf[:0]
}

func (t *materializeOp) NextTuple() tuple {
	if len(t.rows) == 0 {
		flow := t.input.Next()
		if flow.n == 0 {
			return nil
		}
		t.rows = t.rowsBuf
		if flow.useSel {
			for outIdx, cIdx := range t.cols {
				for s := 0; s < flow.n; s++ {
					i := flow.sel[s]
					n := flow.b[cIdx][i]
					t.rows[s][outIdx] = n
				}
			}
		} else {
			for outIdx, cIdx := range t.cols {
				for i := 0; i < flow.n; i++ {
					t.rows[i][outIdx] = flow.b[cIdx][i]
				}
			}
		}
		t.rows = t.rows[:flow.n]
	}
	ret := t.rows[0]
	t.rows = t.rows[1:]
	return ret
}
