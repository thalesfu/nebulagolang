package basictype

import (
	"fmt"
	"reflect"
	"strings"
)

type BasicType struct {
	Name        string
	Length      int
	IndexLength int
}

var (
	Bool     BasicType = BasicType{Name: "BOOL"}
	Int8     BasicType = BasicType{Name: "INT8"}
	Int16    BasicType = BasicType{Name: "INT16"}
	Int32    BasicType = BasicType{Name: "INT32"}
	Int64    BasicType = BasicType{Name: "INT64"}
	Float    BasicType = BasicType{Name: "FLOAT"}
	Double   BasicType = BasicType{Name: "DOUBLE"}
	Date     BasicType = BasicType{Name: "DATE"}
	String   BasicType = BasicType{Name: "STRING", IndexLength: 100}
	Time     BasicType = BasicType{Name: "TIME"}
	Datetime BasicType = BasicType{Name: "DATETIME"}
	Duration BasicType = BasicType{Name: "DURATION"}
)

func (t *BasicType) String() string {
	if t.Name == "FIXED_STRING" {
		return fmt.Sprintf("FIXED_STRING(%d)", t.Length)
	} else {
		return t.Name
	}
}

func FixedString(length int) BasicType {
	return BasicType{Name: "FIXED_STRING", Length: length}
}

func StringWithIndexLength(indexLength int) BasicType {
	return BasicType{Name: "STRING", IndexLength: indexLength}
}

func GetTypeByName(name string) BasicType {
	if name == "" {
		return String
	}

	switch strings.ToUpper(name) {
	case "BOOL":
		return Bool
	case "INT8":
		return Int8
	case "INT16":
		return Int16
	case "INT32":
		return Int32
	case "INT64":
		return Int64
	case "FLOAT":
		return Float
	case "DOUBLE":
		return Double
	case "DATE":
		return Date
	case "STRING":
		return String
	case "TIME":
		return Time
	case "DATETIME":
		return Datetime
	case "DURATION":
		return Duration
	default:
		return String
	}
}

func GetTypeByReflectTypeKind(kd reflect.Kind) BasicType {
	switch kd {
	case reflect.String:
		return String
	case reflect.Int, reflect.Int64:
		return Int64
	case reflect.Int8:
		return Int8
	case reflect.Int16:
		return Int16
	case reflect.Int32:
		return Int32
	case reflect.Float32:
		return Float
	case reflect.Float64:
		return Double
	}

	return String
}

func GetTypeByReflectFieldStruct(fd reflect.StructField) BasicType {
	propertyTypeName := fd.Tag.Get("nebulatype")

	if propertyTypeName != "" {
		return GetTypeByName(propertyTypeName)
	}

	return GetTypeByReflectTypeKind(fd.Type.Kind())
}
