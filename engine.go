package exectoy

type Engine struct {
	source   ExecSource
	pipeline []ExecOp
}

var _ ExecSource = Engine{}

func (e Engine) Init() {
	if len(e.pipeline) == 0 {
		panic("no pipeline")
	}
	e.source.Init()
	for i := range e.pipeline {
		e.pipeline[i].Init()
	}
}

func (e Engine) Next() dataFlow {
	flow := e.source.Next()
	if flow.n == 0 {
		return flow
	}
	for _, op := range e.pipeline {
		flow = op.Next(flow)
		if flow.n == 0 {
			return flow
		}
	}
	return flow
}
