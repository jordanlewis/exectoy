package exectoy

// hashIntOp hashes a column into another column
// TODO(jordan) these are really "projections" where the operator is a hash
// function...
type hashIntOp struct {
	input ExecOp

	colIdx     int
	hashColIdx int
}

func (hashIntOp) Init() {}
func (h hashIntOp) Next() dataFlow {
	flow := h.input.Next()
	if flow.n == 0 {
		return flow
	}

	col := flow.b[h.colIdx].(intColumn)
	hashCol := flow.b[h.hashColIdx].(intColumn)
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			hashCol[i] = col[i]
		}
	} else {
		for i := 0; i < flow.n; i++ {
			hashCol[i] = col[i]
		}
	}
	return flow
}

// rehashIntOp hashes a column and mixes it with an old hashed column
// Algorithm from Architecture-Conscious Hashing
// http://www.cs.cmu.edu/~./damon2006/pdf/zukowski06archconscioushashing.pdf
type rehashIntOp struct {
	input ExecOp

	colIdx     int
	hashColIdx int
}

func (rehashIntOp) Init() {}
func (h rehashIntOp) Next() dataFlow {
	flow := h.input.Next()
	if flow.n == 0 {
		return flow
	}

	col := flow.b[h.colIdx].(intColumn)
	hashCol := flow.b[h.hashColIdx].(intColumn)
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			hash := col[i]
			hashCol[i] ^= (hash << 11) ^ (hash >> 7)
		}
	} else {
		for i := 0; i < flow.n; i++ {
			hash := col[i]
			hashCol[i] ^= (hash << 11) ^ (hash >> 7)
		}
	}
	return flow
}
