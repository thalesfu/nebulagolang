package build

import (
	"github.com/thalesfu/nebulagolang"
	"github.com/thalesfu/nebulagolang/utils"
	"log"
	"reflect"
)

func CreateTagWithIndexes[T interface{}](space *nebulagolang.Space) {
	tag, ok := nebulagolang.BuildTagSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s TAG SCHEMA FAILED%s\n", utils.PrintColorRed, reflect.TypeOf(zeroT).Name(), utils.PrintColorReset)
	}

	r := space.CreateTagWithIndexes(tag)

	if !r.Ok {
		log.Fatalf("%sCREATE %s failed%s\nError Detail: \n%v", utils.PrintColorRed, tag.Comment, utils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, tag.Comment, utils.PrintColorReset)
}

func RebuildTagWithIndexes[T interface{}](space *nebulagolang.Space) {
	tag, ok := nebulagolang.BuildTagSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s TAG SCHEMA FAILED%s\n", utils.PrintColorRed, reflect.TypeOf(zeroT).Name(), utils.PrintColorReset)
	}

	r := space.RebuildTagWithIndexes(tag)

	if !r.Ok {
		log.Fatalf("%sCREATE %s FAILED%s\nERROR DETAIL: \n%v", utils.PrintColorRed, tag.Comment, utils.PrintColorReset, r.Err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, tag.Comment, utils.PrintColorReset)
}
