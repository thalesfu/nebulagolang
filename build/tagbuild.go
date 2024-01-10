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

	ok, err := space.CreateTagWithIndexes(tag)

	if !ok {
		log.Fatalf("%sCREATE %s failed%s\nError Detail: \n%v", utils.PrintColorRed, tag.Comment, utils.PrintColorReset, err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, tag.Comment, utils.PrintColorReset)
}

func RebuildTagWithIndexes[T interface{}](space *nebulagolang.Space) {
	tag, ok := nebulagolang.BuildTagSchema[T]()

	var zeroT T

	if !ok {
		log.Fatalf("%sCREATE %s TAG SCHEMA FAILED%s\n", utils.PrintColorRed, reflect.TypeOf(zeroT).Name(), utils.PrintColorReset)
	}

	ok, err := space.RebuildTagWithIndexes(tag)

	if !ok {
		log.Fatalf("%sCREATE %s FAILED%s\nERROR DETAIL: \n%v", utils.PrintColorRed, tag.Comment, utils.PrintColorReset, err)
	}

	log.Printf("%sCREATE %s SUCCESS%s\n", utils.PrintColorGreen, tag.Comment, utils.PrintColorReset)
}
