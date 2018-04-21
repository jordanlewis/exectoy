package exectoy

// These will get templated implementations!
type projPlusIntIntConst struct {
	input ExecOp

	intIdx   int
	constArg int

	outputIdx int
}

func (p *projPlusIntIntConst) Next() dataFlow {
	flow := p.input.Next()

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

func (p projPlusIntIntConst) Init() {}

type projPlusIntInt struct {
	input ExecOp

	int1Idx int
	int2Idx int

	outputIdx int
}

func (p projPlusIntInt) Next() dataFlow {
	flow := p.input.Next()

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

func (p *projPlusIntInt) Init() {}
