package nebulagolang

import (
	"github.com/thalesfu/golangutils"
	"reflect"
)

func GetTagName[T interface{}]() string {
	return getTagNameByReflectType(golangutils.GetType[T]())
}

func getTagNameByReflectType(t reflect.Type) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		tagName := field.Tag.Get("nebulatagname")
		if tagName != "" {
			return tagName
		}
	}

	return ""
}
