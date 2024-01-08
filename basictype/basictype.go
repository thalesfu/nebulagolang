package basictype

import "fmt"

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
