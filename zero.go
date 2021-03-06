package exectoy

var zeroVec = make(intColumn, batchRowLen)

// This operator zeroes a column.
type zeroIntOp struct {
	input ExecOp

	colIdx int
}

func (z zeroIntOp) Next() dataFlow {
	flow := z.input.Next()
	if flow.n == 0 {
		return flow
	}

	copy(flow.b[z.colIdx].(intColumn), zeroVec)
	return flow
}

func (zeroIntOp) Init() {}
