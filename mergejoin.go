package exectoy

import "fmt"

type mergeJoinIntIntOp struct {
	left  ExecOp
	right ExecOp

	leftEqColIdx  int
	rightEqColIdx int

	leftCols  []int
	rightCols []int

	leftFlow  dataFlow
	rightFlow dataFlow

	leftFlowIdx  int
	rightFlowIdx int

	leftBatchBuf  batch
	rightBatchBuf batch

	leftBatchN  int
	rightBatchN int

	leftBatchIdx  int
	rightBatchIdx int

	d dataFlow
}

func (m *mergeJoinIntIntOp) Init() {
	nOutputCols := len(m.leftCols) + len(m.rightCols)
	if nOutputCols == 0 {
		panic("no output cols")
	}
	m.d.b = make(batch, nOutputCols)
	for i := range m.d.b {
		m.d.b[i] = make(intColumn, batchRowLen)
	}

	m.leftBatchBuf = make(batch, len(m.leftCols))
	m.rightBatchBuf = make(batch, len(m.rightCols))

	for i := range m.leftBatchBuf {
		m.leftBatchBuf[i] = make(intColumn, 0, 1)
	}
	for i := range m.rightBatchBuf {
		m.rightBatchBuf[i] = make(intColumn, 0, 1)
	}
}

func (m *mergeJoinIntIntOp) Next() dataFlow {
	// Check for buffered output.
	if m.maybeOutput() {
		return m.d
	}

	if m.leftFlow.n == 0 {
		fmt.Println("Had to next left")
		m.leftFlow = m.left.Next()
	}
	if m.rightFlow.n == 0 {
		fmt.Println("Had to next right")
		m.rightFlow = m.right.Next()
	}
	fmt.Println("left:", m.leftFlow)
	fmt.Println("right:", m.rightFlow)
	if m.leftFlow.n == 0 || m.rightFlow.n == 0 {
		ret := m.d
		m.d.n = 0
		return ret
	}
	leftCol, rightCol := m.leftFlow.b[m.leftEqColIdx], m.rightFlow.b[m.rightEqColIdx]

	for {
		// todo(jordan) deal with sel
		leftVal, rightVal := leftCol.(intColumn)[m.leftFlowIdx], rightCol.(intColumn)[m.rightFlowIdx]
		fmt.Printf("Enter merge loop. left[%d]: %d, right[%d]: %d\n", m.leftFlowIdx, leftVal, m.rightFlowIdx, rightVal)
		var ok bool
		if leftVal > rightVal {
			ok, m.rightFlow, m.rightFlowIdx = m.advanceToMatch(leftVal, m.rightFlow, m.rightEqColIdx, m.right, m.rightFlowIdx)
			if !ok && m.rightFlowIdx == -1 {
				// ran out of rows on the right.
				fmt.Println("Returning dregs - no more right")
				ret := m.d
				m.d.n = 0
				return ret
			}
		} else if leftVal < rightVal {
			ok, m.leftFlow, m.leftFlowIdx = m.advanceToMatch(rightVal, m.leftFlow, m.leftEqColIdx, m.left, m.leftFlowIdx)
			if !ok && m.leftFlowIdx == -1 {
				// ran out of rows on the left.
				fmt.Println("Returning dregs - no more left")
				ret := m.d
				m.d.n = 0
				return ret
			}
		} else { // leftVal == rightVal
			// buffer rows on both sides.
			m.leftFlow, m.leftFlowIdx = m.bufferMatchGroup(leftVal, m.leftFlow, m.leftEqColIdx, m.left, m.leftFlowIdx, m.leftCols, m.leftBatchBuf)
			m.rightFlow, m.rightFlowIdx = m.bufferMatchGroup(rightVal, m.rightFlow, m.rightEqColIdx, m.right, m.rightFlowIdx, m.rightCols, m.rightBatchBuf)
			fmt.Println("Done buffering. indexes are now", m.leftFlowIdx, m.rightFlowIdx)
			fmt.Println("Done buffering. flows are now", m.leftFlow, m.rightFlow)
			fmt.Println("Done buffering. batchBufs are now", m.leftBatchBuf, m.rightBatchBuf)
			if m.maybeOutput() {
				return m.d
			}
		}
	}
}

func (m *mergeJoinIntIntOp) maybeOutput() bool {
	// cartesian product the buffers to the output.
	avail := batchRowLen - m.d.n
	required := (len(m.leftBatchBuf[0].(intColumn)) - m.leftBatchIdx) * (len(m.rightBatchBuf[0].(intColumn)) - m.rightBatchIdx)
	fmt.Println("Hello from maybeOutput", m.leftBatchIdx, m.rightBatchIdx, avail, required)
	if required == 0 {
		fmt.Println("Nothing to output")
		return false
	}
	toCopy := required
	if avail < required {
		toCopy = avail
	}
	m.d.n += toCopy

	leftBatchIdx, rightBatchIdx := m.leftBatchIdx, m.rightBatchIdx

COLUMNLOOPL:
	for i := range m.leftCols {
		// for each column
		outputCol := m.d.b[i].(intColumn)
		rowIdx := 0
		bufCol := m.leftBatchBuf[i].(intColumn)
		for ; leftBatchIdx < len(bufCol); leftBatchIdx++ {
			// for each value in the left side
			val := bufCol[leftBatchIdx]
			for ; rightBatchIdx < len(m.rightBatchBuf[i].(intColumn)); rightBatchIdx++ {
				// for each value in the right side... copy it!
				outputCol[rowIdx] = val
				rowIdx++
				if rowIdx >= toCopy {
					continue COLUMNLOOPL
				}
			}
			rightBatchIdx = 0
		}
	}

	leftBatchIdx, rightBatchIdx = m.leftBatchIdx, m.rightBatchIdx

COLUMNLOOPR:
	for i := range m.rightCols {
		outputCol := m.d.b[i+len(m.leftCols)].(intColumn)
		rowIdx := 0
		bufCol := m.rightBatchBuf[i].(intColumn)
		for ; leftBatchIdx < len(m.leftBatchBuf[i].(intColumn)); leftBatchIdx++ {
			for ; rightBatchIdx < len(bufCol); rightBatchIdx++ {
				outputCol[rowIdx] = bufCol[rightBatchIdx]
				rowIdx++
				if rowIdx >= toCopy {
					continue COLUMNLOOPR
				}
			}
			rightBatchIdx = 0
		}
	}

	m.leftBatchIdx, m.rightBatchIdx = leftBatchIdx, rightBatchIdx

	if required <= avail {
		fmt.Println("Clearing bufs")
		// We got everything into our batch. Clear the bufs.
		for i := range m.leftBatchBuf {
			m.leftBatchBuf[i] = m.leftBatchBuf[i].(intColumn)[:0]
		}
		for i := range m.rightBatchBuf {
			m.rightBatchBuf[i] = m.rightBatchBuf[i].(intColumn)[:0]
		}
		m.leftBatchIdx, m.rightBatchIdx = 0, 0
	}

	// output if there's no space left in the buffer
	return required >= avail
}

func (m *mergeJoinIntIntOp) bufferMatchGroup(val int, flow dataFlow, colIdx int, op ExecOp, startIdx int, cols intColumn, batchBuf batch) (dataFlow, int) {
	for {
		col := flow.b[colIdx].(intColumn)
		for matchIdx := startIdx; matchIdx < flow.n; matchIdx++ {
			found := col[matchIdx]
			if val != found {
				return flow, matchIdx
			}
			// TODO(jordan) fail. this should be col-oriented.
			// It's hard because this whole process can span batch boundaries.
			// The algorithm should be:
			// for each col:
			//   add value to buffer until group's over or batch ends.
			// if batch ended early, repeat.
			for i, c := range cols {
				batchBuf[i] = append(batchBuf[i].(intColumn), flow.b[c].(intColumn)[matchIdx])
			}
		}
		// If we got here, we made it to the end of the batch. We must retrieve the
		// next batch to ensure there are no more matches in that one.
		fmt.Println("Made it to the end of the batch")
		flow = op.Next()
		if flow.n == 0 {
			return flow, 0
		}
		startIdx = 0
	}
}

// returns false if no match
func (m *mergeJoinIntIntOp) advanceToMatch(val int, flow dataFlow, colIdx int, op ExecOp, startIdx int) (bool, dataFlow, int) {
	for {
		col := flow.b[colIdx].(intColumn)
		for matchIdx := startIdx; matchIdx < flow.n; matchIdx++ {
			found := col[matchIdx]
			if val == found {
				return true, flow, matchIdx
			} else if val < found {
				return false, flow, matchIdx
			}
		}

		flow = op.Next()
		if flow.n == 0 {
			return false, flow, -1
		}
		startIdx = 0
	}
}
