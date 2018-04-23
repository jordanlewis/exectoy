package exectoy

// These will get templated implementations!
type projPlusIntIntConst struct {
	intIdx   int
	constArg int

	outputIdx int
}

var _ ExecOp = &projPlusIntIntConst{}

func (p projPlusIntIntConst) Init() {}

func (p *projPlusIntIntConst) Next(flow dataFlow) dataFlow {
	projCol := flow.b[p.outputIdx]
	intCol := flow.b[p.intIdx]
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			projCol[i] = intCol[i] + p.constArg
		}
	} else {
		for i := 0; i < batchRowLen; i++ {
			projCol[i] = intCol[i] + p.constArg
		}
	}
	return flow
}

type projPlusIntInt struct {
	int1Idx int
	int2Idx int

	outputIdx int
}

var _ ExecOp = &projPlusIntInt{}

func (p *projPlusIntInt) Init() {}

func (p projPlusIntInt) Next(flow dataFlow) dataFlow {
	projCol := flow.b[p.outputIdx]
	col1 := flow.b[p.int1Idx]
	col2 := flow.b[p.int2Idx]
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			projCol[i] = col1[i] + col2[i]
		}
	} else {
		for i := 0; i < flow.n; i++ {
			projCol[i] = col1[i] + col2[i]
		}
	}
	return flow
}
