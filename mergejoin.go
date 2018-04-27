package exectoy

type MergeJoinOp struct {
	left  ExecOp
	right ExecOp

	leftColIdxs  []int
	rightColIdxs []int
}

func (m *MergeJoinOp) Init() {
	m.left.Init()
	m.right.Init()
}

func (m *MergeJoinOp) Next() dataFlow {
	leftFlow := left.Next()
	rightFlow := left.Next()
}
