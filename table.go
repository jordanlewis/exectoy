package exectoy

type materializeOp struct {
	input ExecOp

	cols []int

	rows []tuple
}

func (t *materializeOp) Init() {
	t.rows = make([]tuple, 0, batchRowLen)
	for i := range t.rows {
		t.rows[i] = make(tuple, len(t.cols))
	}
}

func (t *materializeOp) NextRow() tuple {
	if len(t.rows) == 0 {
		flow := t.input.Next()
		if flow.useSel {
			for outIdx, cIdx := range t.cols {
				for s := 0; s < flow.n; s++ {
					i := flow.sel[s]
					t.rows[s][outIdx] = flow.b[cIdx][i]
				}
			}
		} else {
			for outIdx, cIdx := range t.cols {
				for i := 0; i < flow.n; i++ {
					t.rows[i][outIdx] = flow.b[cIdx][i]
				}
			}
		}
	}
	ret := t.rows[0]
	t.rows = t.rows[1:]
	return ret
}
