package exectoy

/*
type sortAllOp struct {
	input ExecOp

	nCols int

	n        int
	buf      []column
	sortCols []int
}

func (s *sortAllOp) Init() dataFlow {
	s.buf = make([]column, nCols)
	for i := range s.buf {
		s.buf[i] = make(column, 10*rowBatchSize)
	}
}

func (s *sortAllOp) fill() {
	for {
		flow := s.input.Next()
		if flow.n == 0 {
			return
		}
	}
}

func (s *sortAllOp) Len() { return s.n }
func (s *sortAllOp) Less(i int, j int) bool {
	return s.n
}

func (s *sortAllOp) Next() dataFlow {
	s.fill()
}
*/
