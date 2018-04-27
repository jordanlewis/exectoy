package exectoy

// sortedDistinctIntIntOp runs a distinct on the column in sortedDistinctCol,
// writing the result to the int column in outputColIdx by or'ing the difference
// between the current and last value in the column with what's already in the
// output column. this has the effect of setting the output column to 0 when
// the input is distinct.
type sortedDistinctIntIntOp struct {
	input ExecOp

	sortedDistinctCol int
	outputColIdx      int

	// set to true if this is the first column in a logical distinct.
	firstColInDistinct bool

	// Starts at 1, flips to 0.
	firstColToLookAt int

	col     column
	lastVal int
}

func (p *sortedDistinctIntIntOp) Init() {
	p.firstColToLookAt = 1
}

func (p *sortedDistinctIntIntOp) Next() dataFlow {
	flow := p.input.Next()
	if flow.n == 0 {
		return flow
	}
	p.col = flow.b[p.sortedDistinctCol]
	outputCol := flow.b[p.outputColIdx]

	// we always output the first row.
	for i := 0; i < p.firstColToLookAt; i++ {
		p.lastVal = p.col[0]
		outputCol[i] = 1
	}

	if flow.useSel {
		for s := p.firstColToLookAt; s < flow.n; s++ {
			i := flow.sel[s]
			/* Morally, we're doing this, but we replace the control dep with a data
			 * dep.
			if col[i] != lastVal {
				outputCol[i] = true
				lastVal = col[i]
			}
			*/
			outputCol[i] |= (p.col[i] - p.lastVal)
			p.lastVal = p.col[i]
		}
	} else {
		for i := p.firstColToLookAt; i < flow.n; i++ {
			/* Morally, we're doing this, but we replace the control dep with a data
			 * dep.
			if col[i] != lastVal {
				outputCol[i] = true
				lastVal = col[i]
			}
			*/
			outputCol[i] |= (p.col[i] - p.lastVal)
			p.lastVal = p.col[i]
		}
	}

	p.firstColToLookAt = 0

	return flow
}

// This finalizer op transforms the vector in outputColIdx to the sel vector,
// by adding an index to sel if it's equal to 0.
type sortedDistinctFinalizerOp struct {
	input ExecOp

	outputColIdx int
}

func (p sortedDistinctFinalizerOp) Next() dataFlow {
	flow := p.input.Next()
	if flow.n == 0 {
		return flow
	}

	outputCol := flow.b[p.outputColIdx]

	// convert outputVec to sel
	idx := 0
	if flow.useSel {
		max := flow.sel[flow.n-1]
		for i := 0; i < max; i++ {
			if outputCol[i] != 0 {
				flow.sel[idx] = i
				idx++
			}
		}
	} else {
		for i := 0; i < flow.n; i++ {
			if outputCol[i] != 0 {
				flow.sel[idx] = i
				idx++
			}
		}
	}

	flow.useSel = true
	flow.n = idx
	return flow
}

func (p sortedDistinctFinalizerOp) Init() {}
