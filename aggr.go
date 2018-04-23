package exectoy

// ordered aggregation
// "direct" aggregation where group keys map to array indexes
// hash aggregation
type aggrOp struct {
	input ExecOp

	funcs []aggrFunc

	accs []aggrMap
}

func (a *aggrOp) Init() {
	accs := make([]aggrMap, len(a.funcs))
	for i := range a.funcs {
		accs[i] = make(aggrMap)
	}
}

type aggrFunc func(d dataFlow, groupHashCol int, m aggrMap)

type aggrMap map[int]aggrAcc

// todo(jordan) we can do better than this right?
type aggrAcc interface{}

type aggrSumIntOp struct {
	input ExecOp

	colIdx int

	acc int
}

func (aggrSumIntOp) Init() {}

func (a *aggrSumIntOp) Next() dataFlow {
	flow := a.input.Next()
	if flow.n == 0 {
		return flow
	}

	col := flow.b[a.colIdx]
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			a.acc += col[i]
		}
	} else {
		for i := 0; i < flow.n; i++ {
			a.acc += col[i]
		}
	}
	return flow
}
