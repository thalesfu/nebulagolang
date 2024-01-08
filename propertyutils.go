package nebulagolang

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"
)

func getPropertyValueAndType(item interface{}) (reflect.Value, reflect.Type) {
	valueOfTag := reflect.ValueOf(item).Elem()

	if valueOfTag.Kind() == reflect.Ptr {
		valueOfTag.Set(reflect.New(valueOfTag.Type().Elem()))
		valueOfTag = valueOfTag.Elem()
	}

	typeOfTag := valueOfTag.Type()

	return valueOfTag, typeOfTag
}

func GetTagPropertiesNamesYieldString[T TagEntity]() string {
	pns := GetPropertiesNames[T]()
	for i, pn := range pns {
		pns[i] = "properties($-.v)." + pn + " AS " + pn
	}

	return fmt.Sprintf("| YIELD id($-.v) as vid, %s", strings.Join(pns, ", "))
}

func GetEdgePropertiesNamesYieldString[T EdgeEntity](edge EdgeEntity) string {
	pns := GetPropertiesNames[T]()
	for i, pn := range pns {
		pns[i] = "properties($-.e)." + pn + " AS " + pn
	}

	ps := []string{"src($-.e) as src", "dst($-.e) as dst"}
	ps = append(ps, pns...)

	return fmt.Sprintf("| YIELD %s", strings.Join(ps, ", "))
}

func GetPropertiesNames[T any]() []string {
	propertiesNames := make([]string, 0)

	var zeroT T
	valueOfT := reflect.ValueOf(zeroT)
	typeOfTag := valueOfT.Type()

	if typeOfTag.Kind() == reflect.Ptr {
		typeOfTag = typeOfTag.Elem()
	}

	for i := 0; i < typeOfTag.NumField(); i++ {
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
		}
	}

	return propertiesNames
}

func GetUpdatePropertiesNamesAndValuesString(tag interface{}) (string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)

	valueOfTag, typeOfTag := getPropertyValueAndType(tag)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if isZeroValue(fv, ft) {
				name := tagProperty + " AS " + tagProperty
				propertiesNames = append(propertiesNames, name)
				value := getFieldValue(ft, fv)
				propertiesValues = append(propertiesValues, tagProperty+" = "+value)
			}
		}
	}

	return strings.Join(propertiesNames, ", "), strings.Join(propertiesValues, ", ")
}

func GetTagUpdateString(t TagEntity) string {
	pns, pvs := GetUpdatePropertiesNamesAndValuesString(t)

	return "UPDATE VERTEX ON " + t.GetTagName() + " \"" + t.VID() + "\" SET " + pvs + " YIELD " + pns + ";"
}

func GetEdgeUpdateString(e EdgeEntity) string {
	pns, pvs := GetUpdatePropertiesNamesAndValuesString(e)

	return "UPDATE EDGE ON " + e.GetEdgeName() + " " + e.EID() + " SET " + pvs + " YIELD " + pns + ";"
}

func isZeroValue(fv reflect.Value, ft reflect.StructField) bool {
	if ft.Type.Kind() == reflect.Bool {
		return true
	}

	return fv.Interface() != reflect.Zero(ft.Type).Interface()
}

func GetInsertPropertiesNamesAndValuesString(tag interface{}) (string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)

	valueOfTag, typeOfTag := getPropertyValueAndType(tag)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if isZeroValue(fv, ft) {
				propertiesNames = append(propertiesNames, tagProperty)
				propertiesValues = append(propertiesValues, getFieldValue(ft, fv))
			}
		}
	}

	return strings.Join(propertiesNames, ", "), strings.Join(propertiesValues, ", ")
}

func GetAllInsertTagWithPropertiesAndPropertyValueList(tag TagEntity) (string, []string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)

	valueOfTag, typeOfTag := getPropertyValueAndType(tag)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
			if isZeroValue(fv, ft) {
				propertiesValues = append(propertiesValues, getFieldValue(ft, fv))
			} else {
				propertiesValues = append(propertiesValues, getDefaultValue(ft, fv))
			}
		}
	}

	return tag.GetTagName() + "(" + strings.Join(propertiesNames, ", ") + ")", propertiesValues
}

func GetAllInsertPropertiesNamesAndValuesString(tag interface{}) (string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)

	valueOfTag, typeOfTag := getPropertyValueAndType(tag)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
			if isZeroValue(fv, ft) {
				propertiesValues = append(propertiesValues, getFieldValue(ft, fv))
			} else {
				propertiesValues = append(propertiesValues, getDefaultValue(ft, fv))
			}
		}
	}

	return strings.Join(propertiesNames, ", "), strings.Join(propertiesValues, ", ")
}

func getDefaultValue(ft reflect.StructField, fv reflect.Value) string {
	switch ft.Type.Kind() {
	case reflect.String:
		return "\"\""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "0"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "0"
	case reflect.Float32, reflect.Float64:
		return "0.0"
	default:
		if ft.Type == reflect.TypeOf(time.Time{}) {
			tagProperty := ft.Tag.Get("nebulatype")
			switch tagProperty {
			case "Date":
				return "DATE(\"2000-01-01\")"
			case "DateTime":
				return "DATETIME(\"2000-01-01 00:00:00\")"
			}

		}
		return fmt.Sprintf("%v", fv.Interface())
	}
}

func getFieldValue(ft reflect.StructField, fv reflect.Value) string {
	switch ft.Type.Kind() {
	case reflect.String:
		return "\"" + escapeSpecialChars(fv.String()) + "\""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", fv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", fv.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", fv.Float())
	default:
		if ft.Type == reflect.TypeOf(time.Time{}) {
			tagProperty := ft.Tag.Get("nebulatype")
			switch tagProperty {
			case "Date":
				return fmt.Sprintf("DATE(\"%s\")", fv.Interface().(time.Time).Format("2006-01-02"))
			case "DateTime":
				return fmt.Sprintf("DATETIME(\"%s\")", fv.Interface().(time.Time).Format("2006-01-02 15:04:05"))
			}

		}
		return fmt.Sprintf("%v", fv.Interface())
	}
}

func escapeSpecialChars(s string) string {
	var buf bytes.Buffer
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\n':
			buf.WriteString("\\n")
		case '\t':
			buf.WriteString("\\t")
		case '"':
			buf.WriteString("\\\"")
		default:
			buf.WriteByte(s[i])
		}
	}
	result := buf.String()
	return result
}
