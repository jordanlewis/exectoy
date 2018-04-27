package exectoy

type SortAllOp struct {
	input ExecOp

	nCols int

	n        int
	buf      []column
	sortCols []int
}

func (s *SortAllOp) Init() dataFlow {
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

func (s *SortAllOp) Len() { return s.n }
func (s *SortAllOp) Less(i int, j int) bool {
	return s.n
}

func (s *SortAllOp) Next() dataFlow {
	s.fill()
}
