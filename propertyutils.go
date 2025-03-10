package nebulagolang

import (
	"bytes"
	"fmt"
	"github.com/thalesfu/golangutils"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	nebulaggonebula "github.com/vesoft-inc/nebula-go/v3/nebula"
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

func GetPropertiesNames(t reflect.Type) []string {
	propertiesNames := make([]string, 0)

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
		}
	}

	return propertiesNames
}

func isZeroValue(fv reflect.Value, ft reflect.StructField) bool {
	if ft.Type.Kind() == reflect.Bool {
		return true
	}

	return fv.Interface() != reflect.Zero(ft.Type).Interface()
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

func MappingRowDataToPropertyValue(ft reflect.StructField, fv reflect.Value, value *nebulaggonebula.Value) {
	switch ft.Type.Kind() {
	case reflect.String:
		fv.SetString(string(value.GetSVal()))
	case reflect.Int:
		fv.SetInt(value.GetIVal())
	case reflect.Int64:
		fv.SetInt(value.GetIVal())
	case reflect.Float32:
		fallthrough
	case reflect.Float64:
		fv.SetFloat(value.GetFVal())
	case reflect.Bool:
		fv.SetBool(value.GetBVal())
	default:
		if ft.Type == reflect.TypeOf(time.Time{}) {
			tagProperty := ft.Tag.Get("nebulatype")
			switch tagProperty {
			case "Date":
				if value.GetDVal() != nil {
					dateString := fmt.Sprintf("%04d-%02d-%02d", value.GetDVal().GetYear(), value.GetDVal().GetMonth(), value.GetDVal().GetDay())
					t, _ := time.Parse("2006-01-02", dateString)
					fv.Set(reflect.ValueOf(t))
				}
			case "DateTime":
				if value.GetDtVal() != nil {
					t, _ := time.Parse("2006-01-02T15:04:05Z", fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02dZ", value.GetDtVal().GetYear(), value.GetDtVal().GetMonth(), value.GetDtVal().GetDay(), value.GetDtVal().GetHour(), value.GetDtVal().GetMinute(), value.GetDtVal().GetSec()))
					fv.Set(reflect.ValueOf(t))
				}
			}
		}
	}
}

func MappingResultToMap(resultSet *nebulago.ResultSet) map[int]map[string]*nebulaggonebula.Value {
	if resultSet == nil || len(resultSet.GetRows()) == 0 {
		return nil
	}

	r := make(map[int]map[string]*nebulaggonebula.Value)
	for ri, row := range resultSet.GetRows() {
		m := make(map[string]*nebulaggonebula.Value)
		for ci, cell := range row.Values {
			m[resultSet.GetColNames()[ci]] = cell
		}
		r[ri] = m
	}

	return r
}

func GetPropertyQueryByPropertyNameAndValue[T interface{}](propertyName string, propertyValue any) string {
	return GetPropertiesByRelfectTypeAndQuery(golangutils.GetType[T](), map[string]any{propertyName: propertyValue})
}

func GetPropertiesQuery[T interface{}](propertiesNamesAndValues map[string]any) string {
	return GetPropertiesByRelfectTypeAndQuery(golangutils.GetType[T](), propertiesNamesAndValues)
}

func GetPropertiesByRelfectTypeAndQuery(t reflect.Type, propertiesNamesAndValues map[string]any) string {
	itemName := getTagNameByReflectType(t)

	if itemName == "" {
		itemName = getEdgeNameByReflectType(t)
	}

	if len(propertiesNamesAndValues) == 0 {
		return ""
	}

	pnvs := make([]string, len(propertiesNamesAndValues))
	i := 0
	for propertyName, propertyValue := range propertiesNamesAndValues {
		pnvs[i] = fmt.Sprintf("%s.%s==%s", itemName, propertyName, getValueString(propertyValue))
		i++
	}

	return strings.Join(pnvs, " AND ")
}

func getValueString(v any) string {
	fv := golangutils.IndirectValue(reflect.ValueOf(v))

	switch fv.Type().Kind() {
	case reflect.String:
		return "\"" + escapeSpecialChars(fv.String()) + "\""
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fmt.Sprintf("%d", fv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return fmt.Sprintf("%d", fv.Uint())
	case reflect.Float32, reflect.Float64:
		return fmt.Sprintf("%f", fv.Float())
	default:
		if fv.Type() == reflect.TypeOf(time.Time{}) {
			t := fv.Interface().(time.Time)

			if t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 && t.Nanosecond() == 0 {
				return fmt.Sprintf("DATE(\"%s\")", fv.Interface().(time.Time).Format("2006-01-02"))
			} else {
				return fmt.Sprintf("DATETIME(\"%s\")", fv.Interface().(time.Time).Format("2006-01-02 15:04:05"))
			}
		}

		return fmt.Sprintf("%v", fv.Interface())
	}
}
