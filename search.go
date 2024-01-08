package nebulagolang

import (
	"errors"
	"fmt"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	nebulaggonebula "github.com/vesoft-inc/nebula-go/v3/nebula"
	"reflect"
	"strings"
	"time"
)

type NoDataError struct {
	message string
}

func (e *NoDataError) Error() string {
	return e.message
}

func NoData(message string) *NoDataError {
	return &NoDataError{
		message: message,
	}
}

var NoDataErr = NoData("Not found data")

func GetEdgeFromVertex[T EdgeEntity](ns *Space, vid string, query string) ([]T, bool, error) {
	var t T

	q := fmt.Sprintf("GO FROM \"%s\" OVER %s YIELD EDGE as e ", vid, t.GetEdgeName())
	if query != "" {
		q = fmt.Sprintf("GO FROM \"%s\" OVER %s WHERE %s YIELD EDGE as e ", vid, t.GetEdgeName(), query)
	}

	result, ok, err := QueryEdgesByEdgeQuery[T](ns, q)

	if !ok {
		return nil, false, err
	}

	if len(result) == 0 {
		return nil, false, NoData("Not found data by vid: " + vid)
	}

	return result, true, nil
}

func FetchVertex[T TagEntity](ns *Space, vid string) (T, bool, error) {
	var t T
	result, ok, err := QueryTagsByTagQuery[T](ns, getFetchTagQuery[T](vid))

	if !ok {
		return t, false, err
	}

	if len(result) == 0 {
		return t, false, NoData("Not found data by vid: " + vid)
	}

	return result[0], true, nil
}

func ExistVertex[T TagEntity](ns *Space, vid string) (bool, error) {
	count, ok, err := CountByQuery(ns, getFetchTagQuery[T](vid))

	if !ok {
		return false, err
	}

	if count == 0 {
		return false, errors.New("Not found data by vid: " + vid)
	}

	return true, nil
}

func getFetchEdgeQuery[T EdgeEntity](eid string) string {
	var t T

	return fmt.Sprintf("FETCH PROP ON %s %s YIELD EDGE AS e", t.GetEdgeName(), eid)
}

func getFetchTagQuery[T TagEntity](vid string) string {
	var t T

	return fmt.Sprintf("FETCH PROP ON %s \"%s\" YIELD VERTEX AS v", t.GetTagName(), vid)
}

func LookUpVertexByQuery[T TagEntity](ns *Space, query string) ([]T, bool, error) {
	return QueryTagsByTagQuery[T](ns, GetLookupTagQuery[T](query))
}

func LookUpVertexCountByQuery[T TagEntity](ns *Space, query string) (int64, bool, error) {
	return CountByQuery(ns, GetLookupTagQuery[T](query))
}

func LookUpVertexExistByQuery[T TagEntity](ns *Space, query string) (bool, bool, error) {
	count, ok, err := CountByQuery(ns, GetLookupTagQuery[T](query))

	if !ok {
		return false, false, err
	}

	return count > 0, true, nil
}

func FetchEdge[T EdgeEntity](ns *Space, eid string) (T, bool, error) {
	var t T

	result, ok, err := QueryEdgesByEdgeQuery[T](ns, getFetchEdgeQuery[T](eid))

	if !ok {
		return t, false, err
	}

	if len(result) == 0 {
		return t, false, NoData("Not found data by vid: " + eid)
	}

	return result[0], true, nil
}

func ExistEdge[T EdgeEntity](ns *Space, eid string) (bool, error) {
	count, ok, err := CountByQuery(ns, getFetchEdgeQuery[T](eid))

	if !ok {
		return false, err
	}

	if count == 0 {
		return false, errors.New("Not found data by vid: " + eid)
	}

	return true, nil
}

func LookUpEdgeCountByQuery[T EdgeEntity](ns *Space, query string) (int64, bool, error) {
	return CountByQuery(ns, getLookupEdgeQuery[T](query))
}

func LookUpEdgeExistByQuery[T EdgeEntity](ns *Space, query string) (bool, bool, error) {
	count, ok, err := CountByQuery(ns, getLookupEdgeQuery[T](query))

	if !ok {
		return false, false, err
	}

	return count > 0, true, nil
}

func getLookupEdgeQuery[T EdgeEntity](query string) string {
	var t T

	if query == "" {
		return fmt.Sprintf("LOOKUP ON %s YIELD EDGE AS e ", t.GetEdgeName())
	}

	return fmt.Sprintf("LOOKUP ON %s WHERE %s YIELD EDGE AS e ", t.GetEdgeName(), query)
}

func GetLookupTagQuery[T TagEntity](query string) string {
	var t T

	if query == "" {
		return fmt.Sprintf("LOOKUP ON %s YIELD VERTEX AS v ", t.GetTagName())
	}

	return fmt.Sprintf("LOOKUP ON %s WHERE %s YIELD VERTEX AS v ", t.GetTagName(), query)
}

func QueryTagsByTagQuery[T TagEntity](ns *Space, tagQuery string) ([]T, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		tagQuery + GetTagPropertiesNamesYieldString[T]() + ";",
	}

	resultSet, ok, err := ns.Execute(strings.Join(command, ""))

	if !ok {
		return nil, false, err
	}

	if len(resultSet.GetRows()) == 0 {
		return nil, false, errors.New("Not found data by command: " + strings.Join(command, ""))
	}

	data := MappingResultToMap(resultSet)

	result := make([]T, len(data))

	for i, d := range data {
		var ti T
		ti = MappingResultToTag[T](d)
		vid := d["vid"].GetSVal()
		ti.SetVID(string(vid))
		result[i] = ti
	}

	return result, true, nil
}

func QueryEdgesByEdgeQuery[T EdgeEntity](ns *Space, tagQuery string) ([]T, bool, error) {
	var t T

	command := []string{
		"USE " + ns.Name + ";",
		tagQuery + GetEdgePropertiesNamesYieldString[T](t) + ";",
	}

	resultSet, ok, err := ns.Execute(strings.Join(command, ""))

	if !ok {
		return nil, false, err
	}

	if len(resultSet.GetRows()) == 0 {
		return nil, false, NoData(strings.Join(command, ""))
	}

	data := MappingResultToMap(resultSet)

	result := make([]T, len(data))

	for i, d := range data {
		var ei T
		ei = MappingResultToEdge[T](d)
		src := d["src"].GetSVal()
		ei.SetStartVID(string(src))
		dst := d["dst"].GetSVal()
		ei.SetEndVID(string(dst))
		result[i] = ei
	}

	return result, true, nil
}

func CountByQuery(ns *Space, query string) (int64, bool, error) {
	command := []string{
		"USE " + ns.Name + ";",
		query + " | yield count(1) as count;",
	}

	resultSet, ok, err := ns.Execute(strings.Join(command, ""))

	if !ok {
		return 0, false, err
	}

	if len(resultSet.GetRows()) == 0 {
		return 0, false, errors.New("Not found data by command: " + strings.Join(command, ""))
	}

	values, err := resultSet.GetValuesByColName("count")

	if err != nil {
		return 0, false, err
	}

	value, err := values[0].AsInt()

	if err != nil {
		return 0, false, err
	}

	return value, true, nil
}

func MappingResultToTag[T TagEntity](rowData map[string]*nebulaggonebula.Value) T {
	var tag T
	t := tag.New()
	valueOfTag, typeOfTag := getPropertyValueAndType(t)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if rowData[tagProperty] != nil {
				MappingRowDataToPropertyValue(ft, fv, rowData[tagProperty])
			}
		}
	}

	return t.(T)
}

func MappingResultToEdge[T EdgeEntity](rowData map[string]*nebulaggonebula.Value) T {
	edge := new(T)
	valueOfTag, typeOfTag := getPropertyValueAndType(edge)

	for i := 0; i < typeOfTag.NumField(); i++ {
		fv := valueOfTag.Field(i)
		ft := typeOfTag.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if rowData[tagProperty] != nil {
				MappingRowDataToPropertyValue(ft, fv, rowData[tagProperty])
			}
		}
	}

	return *edge
}

func MappingRowDataToPropertyValue(ft reflect.StructField, fv reflect.Value, value *nebulaggonebula.Value) {
	switch ft.Type.Kind() {
	case reflect.String:
		fv.SetString(string(value.GetSVal()))
	case reflect.Int:
		fv.SetInt(value.GetIVal())
	case reflect.Int64:
		fv.SetInt(value.GetIVal())
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
					t, _ := time.Parse("2006-01-02", fmt.Sprintf("%d-%d-%d", value.GetDVal().GetYear(), value.GetDVal().GetMonth(), value.GetDVal().GetDay()))
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
