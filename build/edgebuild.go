package build

import (
	"github.com/thalesfu/golangutils"
	"github.com/thalesfu/nebulagolang"
	"log"
	"reflect"
)

func CreateEdgeWithIndexes[T interface{}](space *nebulagolang.Space) {
	edge, ok := nebulagolang.BuildEdgeSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s EDGE SCHEMA FAILED%s\n", golangutils.PrintColorRed, reflect.TypeOf(zeroT).Name(), golangutils.PrintColorReset)
	}

	r := space.CreateEdgeWithIndexes(edge)

	if !r.Ok {
		log.Fatalf("%sCREATE %s failed%s\nError Detail: \n%v", golangutils.PrintColorRed, edge.Comment, golangutils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", golangutils.PrintColorGreen, edge.Comment, golangutils.PrintColorReset)
}

func RebuildEdgeWithIndexes[T interface{}](space *nebulagolang.Space) {
	edge, ok := nebulagolang.BuildEdgeSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s EDGE SCHEMA FAILED%s\n", golangutils.PrintColorRed, reflect.TypeOf(zeroT).Name(), golangutils.PrintColorReset)
	}

	r := space.RebuildEdgeWithIndexes(edge)

	if !r.Ok {
		log.Fatalf("%sCREATE %s FAILED%s\nERROR DETAIL: \n%v", golangutils.PrintColorRed, edge.Comment, golangutils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", golangutils.PrintColorGreen, edge.Comment, golangutils.PrintColorReset)
}
