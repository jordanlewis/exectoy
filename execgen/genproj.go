package main

import (
	"io"
	"text/template"
)

func genProj(wr io.Writer) error {
	tmpl, err := template.New("proj").Parse(`// Code generated by execgen; DO NOT EDIT.

package exectoy

{{define "opConstName"}}proj{{.Name}}{{.LTyp.Name}}{{.RTyp.Name}}ConstOp{{end}}
{{define "opName"}}proj{{.Name}}{{.LTyp.Name}}{{.RTyp.Name}}Op{{end}}

{{range .}}
{{range .}}

type {{template "opConstName" .}} struct {
	input ExecOp

	colIdx   int
	constArg {{.RTyp.GoTyp}}

	outputIdx int
}

func (p *{{template "opConstName" .}}) Next() dataFlow {
	flow := p.input.Next()

	projCol := flow.b[p.outputIdx].({{.RetTyp.GoTyp}}Column)
	col := flow.b[p.colIdx].({{.LTyp.GoTyp}}Column)
	n := flow.n
	if flow.useSel {
		for s := 0; s < n; s++ {
			i := flow.sel[s]
			projCol[i] = col[i] {{.OpStr}} p.constArg
		}
	} else {
		for i := 0; i < n; i++ {
			projCol[i] = col[i] {{.OpStr}} p.constArg
		}
	}
	return flow
}

func (p {{template "opConstName" .}}) Init() {}

type {{template "opName" .}} struct {
	input ExecOp

	col1Idx int
	col2Idx int

	outputIdx int
}

func (p *{{template "opName" .}}) Next() dataFlow {
	flow := p.input.Next()

	projCol := flow.b[p.outputIdx].({{.RetTyp.GoTyp}}Column)
	col1 := flow.b[p.col1Idx].({{.LTyp.GoTyp}}Column)
	col2 := flow.b[p.col2Idx].({{.RTyp.GoTyp}}Column)
	n := flow.n
	if flow.useSel {
		for s := 0; s < n; s++ {
			i := flow.sel[s]
			projCol[i] = col1[i] {{.OpStr}} col2[i]
		}
	} else {
		for i := 0; i < n; i++ {
			projCol[i] = col1[i] {{.OpStr}} col2[i]
		}
	}
	return flow
}

func (p {{template "opName" .}}) Init() {}

{{end}}
{{end}}
`)
	if err != nil {
		return err
	}

	return tmpl.Execute(wr, opMap)
}