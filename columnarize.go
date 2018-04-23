package exectoy

type staticTupleSource struct {
	tuples []tuple
}

func (s *staticTupleSource) NextTuple() tuple {
	if len(s.tuples) == 0 {
		return nil
	}
	t := s.tuples[0]
	s.tuples = s.tuples[1:]
	return t
}

// columnarizeOp takes tuples and turns them into a dataFlow.
type columnarizeOp struct {
	input TupleSource

	numCols       int
	internalBatch batch
	internalSel   column
}

var _ ExecSource = &columnarizeOp{}

func (c *columnarizeOp) Init() {
	b := make([]int, c.numCols*batchRowLen)
	c.internalBatch = make(batch, c.numCols)
	c.internalSel = make(column, batchRowLen)
	for i := range c.internalBatch {
		c.internalBatch[i] = column(b[i*batchRowLen : (i+1)*batchRowLen])
	}
}

func (c columnarizeOp) Next() dataFlow {
	d := dataFlow{
		b:      c.internalBatch,
		sel:    c.internalSel,
		useSel: false,
	}
	for d.n < batchRowLen {
		t := c.input.NextTuple()
		if t == nil {
			break
		}
		for i := range t {
			c.internalBatch[i][d.n] = t[i]
		}
		d.n++
	}
	return d
}
