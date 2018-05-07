package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"io"
	"os"
)

type op int

const (
	invalidOp op = iota
	plusOp
	minusOp
	mulOp
	divOp

	eqOp
	neOp
	ltOp
	lteOp
	gtOp
	gteOp
)

type typ int

const (
	invalidTyp typ = iota
	intTyp
	doubleTyp
	boolTyp
)

type typDef struct {
	Name  string
	GoTyp string
}

var typs = map[typ]typDef{
	intTyp: typDef{
		Name:  "Int",
		GoTyp: "int",
	},
	doubleTyp: typDef{
		Name:  "Double",
		GoTyp: "float64",
	},
	boolTyp: typDef{
		Name:  "Bool",
		GoTyp: "bool",
	},
}

var opNames = map[op]string{
	plusOp:  "Plus",
	minusOp: "Minus",
	mulOp:   "Mul",
	eqOp:    "EQ",
	neOp:    "NE",
	ltOp:    "LT",
	lteOp:   "LTE",
	gtOp:    "GT",
	gteOp:   "GTE",
}

type overload struct {
	Name   string
	OpStr  string
	LTyp   typDef
	RTyp   typDef
	RetTyp typDef
}

func makeOverload(t typ, opStr string) overload {
	return overload{
		OpStr:  opStr,
		LTyp:   typs[t],
		RTyp:   typs[t],
		RetTyp: typs[t],
	}
}

func makePredOverload(t typ, opStr string) overload {
	return overload{
		OpStr:  opStr,
		LTyp:   typs[t],
		RTyp:   typs[t],
		RetTyp: typs[boolTyp],
	}
}

var opMap = map[op][]overload{
	plusOp: {
		makeOverload(intTyp, "+"),
		makeOverload(doubleTyp, "+"),
	},
	minusOp: {
		makeOverload(intTyp, "-"),
		makeOverload(doubleTyp, "-"),
	},
	mulOp: {
		makeOverload(intTyp, "-"),
		makeOverload(doubleTyp, "-"),
	},
	divOp: {
		makeOverload(intTyp, "/"),
		makeOverload(doubleTyp, "/"),
	},
	eqOp: {
		makePredOverload(intTyp, "=="),
		makePredOverload(doubleTyp, "=="),
	},
	neOp: {
		makePredOverload(intTyp, "!="),
		makePredOverload(doubleTyp, "!="),
	},
	ltOp: {
		makePredOverload(intTyp, "<"),
		makePredOverload(doubleTyp, "<"),
	},
	lteOp: {
		makePredOverload(intTyp, "<="),
		makePredOverload(doubleTyp, "<="),
	},
	gtOp: {
		makePredOverload(intTyp, ">"),
		makePredOverload(doubleTyp, ">"),
	},
	gteOp: {
		makePredOverload(intTyp, ">="),
		makePredOverload(doubleTyp, ">="),
	},
}

func init() {
	for i := range opMap {
		for j := range opMap[i] {
			opMap[i][j].Name = opNames[i]
		}
	}
}

var out = flag.String("out", "", "output file")

func usage() {
	fmt.Fprintf(os.Stderr, `usage: execgen [-out filename] command

The commands are:
	proj    generate projection code

`)
	flag.PrintDefaults()
	os.Exit(2)
}

var generators = map[string]func(io.Writer) error{
	"proj": genProj,
	"sel":  genSel,
}

func main() {
	flag.Parse()
	flag.Usage = usage
	if len(flag.Args()) != 1 {
		usage()
	}

	cmd := flag.Args()[0]

	gen := generators[cmd]
	if gen == nil {
		usage()
	}

	var buf bytes.Buffer
	if err := gen(&buf); err != nil {
		panic(err)
	}
	b, err := format.Source(buf.Bytes())
	if err != nil {
		// Write out incorrect source for easier debugging.
		b = buf.Bytes()
		fmt.Printf("Code formatting failed with Go parse error: %s", out, err)
	}

	wr := os.Stdout
	if *out != "" {
		file, err := os.Create(*out)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		wr = file
	}

	wr.Write(b)
}
