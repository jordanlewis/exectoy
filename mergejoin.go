package exectoy

type mergeJoinIntIntOp struct {
	left  ExecOp
	right ExecOp

	leftEqColIdx  int
	rightEqColIdx int

	leftCols  []int
	rightCols []int

	// used if a match group spans batch boundaries
	leftBatchBuf  batch
	rightBatchBuf batch

	nOutputCols int
	d           dataFlow
}

func (m *mergeJoinIntIntOp) Init() {
	m.d.b = make(batch, m.nOutputCols*batchRowLen)

	leftBatchBuf = make(batch, 0, len(leftCols)*batchRowLen)
	rightBatchBuf = make(batch, 0, len(leftCols)*batchRowLen)
}

func (m *mergeJoinIntIntOp) Next() dataFlow {
	leftFlow := m.left.Next()
	rightFlow := m.left.Next()

	if leftFlow.n == 0 || leftFlow.n == 0 {
		// && nothing left to output
		return dataFlow{}
	}
	leftCol, rightCol := leftFlow.b[m.leftEqColIdx], rightFlow.b[m.rightEqColIdx]

	leftIdx, rightIdx := 0, 0

	for {
		// todo(jordan) deal with sel
		leftVal, rightVal := leftCol[leftIdx], rightCol[rightIdx]
		matchIdx := 0
		var ok bool
		if leftVal > rightVal {
			ok, rightFlow, rightIdx = m.advanceToMatch(leftVal, rightFlow, m.rightEqColIdx, m.right)
			if !ok && matchIdx == -1 {
				// ran out of rows on the right.
				return dataFlow{}
			}
		} else if leftVal < rightVal {
			ok, leftFlow, leftIdx = m.advanceToMatch(rightVal, leftFlow, m.leftEqColIdx, m.left)
			if !ok && matchIdx == -1 {
				// ran out of rows on the left.
				return dataFlow{}
			}
		} else { // leftVal != rightVal
			// buffer rows on both sides.

		}
	}

	return dataFlow{}
}

func (m *mergeJoinIntIntOp) bufferMatchGroup(val int, flow dataFlow, colIdx int, op ExecOp, startIdx int, cols []column, batchBuf batch) dataFlow {
	for {
		bufIdx := 0
		for matchIdx := startIdx; matchIdx < flow.n; matchIdx++ {
			found := flow.b[colIdx][matchIdx]
			if val != found {
				return flow
			}
			// TODO(jordan) fail. this should be col-oriented.
			// It's hard because this whole process can span batch boundaries.
			for i := range cols {
				batchBuf[i][matchIdx-startIdx] = flow.b[i][matchIdx]
			}
		}
		// If we got here, we made it to the end of the batch. We must retrieve the
		// next batch to ensure there are no more matches in that one.
		flow = op.Next()
		if flow.n == 0 {
			return flow
		}
		startIdx = 0
	}
	return flow
}

// returns false if no match
func (m *mergeJoinIntIntOp) advanceToMatch(val int, flow dataFlow, colIdx int, op ExecOp, startIdx int) (bool, dataFlow, int) {
	for {
		for matchIdx := startIdx; matchIdx < flow.n; matchIdx++ {
			found := flow.b[colIdx][matchIdx]
			if val == found {
				return true, flow, matchIdx
			} else if val > found {
				return false, flow, matchIdx
			} else if val < found {
				panic("out of order")
			}
		}

		flow = op.Next()
		if flow.n == 0 {
			return false, flow, -1
		}
		startIdx = 0
	}
	panic("fail")
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
