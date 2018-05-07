package main

import (
	"fmt"
	"io"
	"text/template"
)

func genSel(wr io.Writer) error {
	tmpl, err := template.New("sel").Parse(`// Code generated by execgen; DO NOT EDIT.

package exectoy

{{define "opConstName"}}sel{{.Name}}{{.LTyp.Name}}{{.RTyp.Name}}ConstOp{{end}}
{{define "opName"}}sel{{.Name}}{{.LTyp.Name}}{{.RTyp.Name}}Op{{end}}

{{- range .}}
{{- range .}}
{{- if eq .RetTyp.Name "Bool"}}
type {{template "opConstName" .}} struct {
	input ExecOp

	col1Idx   int
	constArg {{.RTyp.GoTyp}}
}

func (p *{{template "opConstName" .}}) Next() dataFlow {
	flow := p.input.Next()

	col1 := flow.b[p.col1Idx].({{.LTyp.GoTyp}}Column)
	idx := 0
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			if col1[i] {{.OpStr}} p.constArg {
				flow.sel[idx] = i
				idx++
			}
		}
	} else {
		for i := 0; i < flow.n; i++ {
			if col1[i] {{.OpStr}} p.constArg {
				flow.sel[idx] = i
				idx++
			}
		}
	}
	flow.n = idx
	flow.useSel = true
	return flow
}

func (p {{template "opConstName" .}}) Init() {}

type {{template "opName" .}} struct {
	input ExecOp

	col1Idx int
	col2Idx int
}

func (p *{{template "opName" .}}) Next() dataFlow {
	flow := p.input.Next()

	col1 := flow.b[p.col1Idx].({{.LTyp.GoTyp}}Column)
	col2 := flow.b[p.col2Idx].({{.RTyp.GoTyp}}Column)

	idx := 0
	if flow.useSel {
		for s := 0; s < flow.n; s++ {
			i := flow.sel[s]
			if col1[i] {{.OpStr}} col2[i] {
				flow.sel[idx] = i
				idx++
			}
		}
	} else {
		for i := 0; i < flow.n; i++ {
			if col1[i] {{.OpStr}} col2[i] {
				flow.sel[idx] = i
				idx++
			}
		}
		flow.useSel = true
	}
	flow.n = idx
	return flow
}

func (p {{template "opName" .}}) Init() {}
{{- end}}
{{- end}}
{{- end}}
`)
	if err != nil {
		return err
	}

	fmt.Println("hi")
	return tmpl.Execute(wr, opMap)
}