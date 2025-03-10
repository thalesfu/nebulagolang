package build

import (
	"github.com/thalesfu/golangutils"
	"github.com/thalesfu/nebulagolang"
	"log"
	"reflect"
)

func CreateTagWithIndexes[T interface{}](space *nebulagolang.Space) {
	tag, ok := nebulagolang.BuildTagSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s TAG SCHEMA FAILED%s\n", golangutils.PrintColorRed, reflect.TypeOf(zeroT).Name(), golangutils.PrintColorReset)
	}

	r := space.CreateTagWithIndexes(tag)

	if !r.Ok {
		log.Fatalf("%sCREATE %s failed%s\nError Detail: \n%v", golangutils.PrintColorRed, tag.Comment, golangutils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", golangutils.PrintColorGreen, tag.Comment, golangutils.PrintColorReset)
}

func RebuildTagWithIndexes[T interface{}](space *nebulagolang.Space) {
	tag, ok := nebulagolang.BuildTagSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s TAG SCHEMA FAILED%s\n", golangutils.PrintColorRed, reflect.TypeOf(zeroT).Name(), golangutils.PrintColorReset)
	}

	r := space.RebuildTagWithIndexes(tag)

	if !r.Ok {
		log.Fatalf("%sCREATE %s FAILED%s\nERROR DETAIL: \n%v", golangutils.PrintColorRed, tag.Comment, golangutils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", golangutils.PrintColorGreen, tag.Comment, golangutils.PrintColorReset)
}
