package exectoy

type mergeJoinIntIntOp struct {
	left  ExecOp
	right ExecOp

	leftEqColIdx  int
	rightEqColIdx int

	nOutputCols int
	b           batch
}

func (m *mergeJoinIntIntOp) Init() {
	m.b = make(nOutputCols * rowBatchLen)
}

func (m *mergeJoinIntIntOp) Next() dataFlow {
	leftFlow := left.Next()
	rightFlow := left.Next()

}

type sortedIntGroupOp struct {
	right ExecOp

	colIdx      int
	groupColIdx int
}

func (m *sortedIntGroupOp) Init() {
}

func (m *sortedIntGroupOp) Next() dataFlow {
	leftFlow := left.Next()
	rightFlow := left.Next()
}
