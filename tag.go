package nebulagolang

import (
	"github.com/thalesfu/nebulagolang/utils"
	"reflect"
)

func GetTagName[T interface{}]() string {
	return getTagNameByReflectType(utils.GetType[T]())
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
