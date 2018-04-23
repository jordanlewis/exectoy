package exectoy

type sortedDistinctOperator struct {
	input ExecOp

	sortedDistinctCols []int

	cols      []column
	lastVal   tuple
	outputVec []int
}

var zeroVec = make([]int, batchRowLen)

func (p *sortedDistinctOperator) Init() {
	p.cols = make([]column, len(p.sortedDistinctCols))
	p.lastVal = make(tuple, len(p.sortedDistinctCols))
	p.outputVec = make([]int, batchRowLen)
}

func (p *sortedDistinctOperator) Next() dataFlow {
	copy(p.outputVec, zeroVec)
	// outputBitmap contains row indexes that we will output
	flow := p.input.Next()
	if flow.n == 0 {
		return flow
	}
	for i, c := range p.sortedDistinctCols {
		p.cols[i] = flow.b[c]
	}
	// TODO(jordan) p.lastVal is wrong in the very first invocation.

	if flow.useSel {
		for cIdx, col := range p.cols {
			lastVal := p.lastVal[cIdx]
			for s := 0; s < flow.n; s++ {
				i := flow.sel[s]
				/* Morally, we're doing this, but we replace the control dep with a data
				 * dep.
				if col[i] != lastVal {
					p.outputVec[i] = true
					lastVal = col[i]
				}
				*/
				p.outputVec[i] |= (col[i] - lastVal)
				lastVal = col[i]
			}
			p.lastVal[cIdx] = lastVal
		}
	} else {
		for cIdx, col := range p.cols {
			lastVal := p.lastVal[cIdx]
			for i := 0; i < flow.n; i++ {
				/* Morally, we're doing this, but we replace the control dep with a data
				 * dep.
				if col[i] != lastVal {
					p.outputVec[i] = true
					lastVal = col[i]
				}
				*/
				p.outputVec[i] |= (col[i] - lastVal)
				lastVal = col[i]
			}
			p.lastVal[cIdx] = lastVal
		}
	}

	// convert outputVec to sel
	idx := 0
	if flow.useSel {
		max := flow.sel[flow.n-1]
		for i := 0; i < max; i++ {
			if p.outputVec[i] != 0 {
				flow.sel[idx] = i
				idx++
			}
		}
	} else {
		for i := 0; i < flow.n; i++ {
			if p.outputVec[i] != 0 {
				flow.sel[idx] = i
				idx++
			}
		}
	}

	flow.useSel = true
	flow.n = idx
	return flow
}
