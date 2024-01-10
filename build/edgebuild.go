package build

import (
	"github.com/thalesfu/nebulagolang"
	"github.com/thalesfu/nebulagolang/utils"
	"log"
	"reflect"
)

func CreateEdgeWithIndexes[T interface{}](space *nebulagolang.Space) {
	edge, ok := nebulagolang.BuildEdgeSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s EDGE SCHEMA FAILED%s\n", utils.PrintColorRed, reflect.TypeOf(zeroT).Name(), utils.PrintColorReset)
	}

	ok, err := space.CreateEdgeWithIndexes(edge)

	if !ok {
		log.Fatalf("%sCREATE %s failed%s\nError Detail: \n%v", utils.PrintColorRed, edge.Comment, utils.PrintColorReset, err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, edge.Comment, utils.PrintColorReset)
}

func RebuildEdgeWithIndexes[T interface{}](space *nebulagolang.Space) {
	edge, ok := nebulagolang.BuildEdgeSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s EDGE SCHEMA FAILED%s\n", utils.PrintColorRed, reflect.TypeOf(zeroT).Name(), utils.PrintColorReset)
	}

	ok, err := space.RebuildEdgeWithIndexes(edge)

	if !ok {
		log.Fatalf("%sCREATE %s FAILED%s\nERROR DETAIL: \n%v", utils.PrintColorRed, edge.Comment, utils.PrintColorReset, err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, edge.Comment, utils.PrintColorReset)
}
