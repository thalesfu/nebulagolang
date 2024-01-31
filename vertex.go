package nebulagolang

import (
	"errors"
	"fmt"
	"github.com/samber/lo"
	"github.com/thalesfu/nebulagolang/utils"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	nebulaggonebula "github.com/vesoft-inc/nebula-go/v3/nebula"
	"reflect"
	"strings"
)

func InsertVertexes[T interface{}](space *Space, vs ...T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	ok, err := IsVertex[T]()
	if !ok {
		return NewErrorResult(err)
	}

	return space.Execute(vertexInsertCommand(vs...))
}

func BatchInsertVertexes[T interface{}](space *Space, batch int, vs []T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	ok, err := IsVertex[T]()
	if !ok {
		return NewErrorResult(err)
	}

	cmds := make([]string, 0)
	chunk := lo.Chunk(vs, batch)

	for i, c := range chunk {
		r := InsertVertexes(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch insert %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func UpdateVertexes[T interface{}](space *Space, vs ...T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	commands := make([]string, len(vs))
	for i, v := range vs {
		commands[i] = vertexUpdateCommand(v)
	}

	return space.Execute(commands...)
}

func BatchUpdateVertexes[T interface{}](space *Space, batch int, vs []T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	chunk := lo.Chunk(vs, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := UpdateVertexes(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch update %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func UpsertVertexes[T interface{}](space *Space, vs ...T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	commands := make([]string, len(vs))
	for i, v := range vs {
		commands[i] = vertexUpsertCommand(v)
	}

	return space.Execute(commands...)
}

func BatchUpsertVertexes[T interface{}](space *Space, batch int, vs []T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	chunk := lo.Chunk(vs, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := UpsertVertexes(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch upsert %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func DeleteVertexes[T interface{}](space *Space, vs ...T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	return space.Execute(vertexDeleteByVertexesVidsCommand(vs...))
}

func BatchDeleteVertexes[T interface{}](space *Space, batch int, vs []T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	ok, err := IsVertex[T]()
	if !ok {
		return NewErrorResult(err)
	}

	cmds := make([]string, 0)
	chunk := lo.Chunk(vs, batch)

	for i, c := range chunk {
		r := DeleteVertexes(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch delete %d vertexes from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return NewSuccessResult(cmds...)
}

func DeleteVertexesByVids(space *Space, vids ...string) *Result {
	if len(vids) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	return space.Execute(vertexDeleteByVidsCommand(vids...))
}

func DeleteVertexesWithEdges[T interface{}](space *Space, vs ...T) *Result {
	if len(vs) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	return space.Execute(vertexDeleteWithEdgeByVertexesVidsCommand(vs...))
}

func DeleteVertexesWithEdgesByVids(space *Space, vids ...string) *Result {
	if len(vids) == 0 {
		return NewErrorResult(errors.New("no vertexes"))
	}

	return space.Execute(vertexDeleteWithEdgeByVidsCommand(vids...))
}

func DeleteAllVertexesByTag[T interface{}](space *Space) *Result {
	return DeleteAllVertexesByQuery[T](space, "")
}

func DeleteAllVertexesByQuery[T interface{}](space *Space, query string) *Result {
	return DeleteVertexByQuery(space, AllVertexesVidsByQueryCommand(utils.GetType[T](), query))
}
func DeleteVertexByQuery(space *Space, query string) *Result {
	return space.Execute(vertexesDeleteByQueryCommand(query))
}

func DeleteVertexWithEdgeByQuery(space *Space, vertexQuery string) *Result {
	return space.Execute(vertexesDeleteWithEdgeByQueryCommand(vertexQuery))
}

func DeleteAllVertexesWithEdgesByTag[T interface{}](space *Space) *Result {
	return DeleteVertexWithEdgeByQuery(space, AllVertexesVidsByQueryCommand(utils.GetType[T](), ""))
}

func DeleteAllVertexesWithEdgesByQuery[T interface{}](space *Space, query string) *Result {
	return DeleteVertexWithEdgeByQuery(space, AllVertexesVidsByQueryCommand(utils.GetType[T](), query))
}

func LoadVertex[T interface{}](space *Space, t T) *Result {
	r := FetchVertexData(space, utils.GetType[T](), GetVID(t))

	if !r.Ok {
		return r
	}

	if len(r.DataSet.GetRows()) == 0 {
		r.Ok = false
		r.Err = NoData("Not found data by command: " + strings.Join(r.Commands, ""))
		return r
	}

	LoadVertexFromResult(r.DataSet, t)

	return r
}

func LoadVertexFromResult[T interface{}](result *nebulago.ResultSet, vertex T) {
	LoadDataToVertexReflectValueFromDataset(reflect.ValueOf(vertex), result)
}

func FetchVertexData(space *Space, t reflect.Type, vid string) *Result {
	return QueryByVertexQuery(space, t, FetchVertexByVidCommand(t, vid))
}

func QueryByVertexQuery(space *Space, t reflect.Type, tagQuery string) *Result {
	return space.Execute(QueryByVertexQueryCommand(t, tagQuery))
}

func GetVertexByVid[T interface{}](space *Space, vid string) *ResultT[T] {
	r := FetchVertexData(space, utils.GetType[T](), vid)

	if !r.Ok {
		return NewResultT[T](r)
	}

	if len(r.DataSet.GetRows()) == 0 {
		r.Ok = false
		r.Err = NoData("Not found data by command: " + strings.Join(r.Commands, ""))
		return NewResultT[T](r)
	}

	data := BuildNewVertexFromResult[T](r.DataSet)

	return NewResultTWithData(r, data)
}

func GetAllVertexesByVertexType[T interface{}](space *Space) *ResultT[map[string]T] {
	return GetAllVertexesByQuery[T](space, "")
}

func GetAllVertexesByQuery[T interface{}](space *Space, query string) *ResultT[map[string]T] {
	return QueryVertexesByQueryToMap[T](space, LookupTagQueryCommand(utils.GetType[T](), query))
}

func QueryVertexesByQueryToMap[T interface{}](space *Space, query string) *ResultT[map[string]T] {
	resultSlice := QueryVertexesByQueryToSlice[T](space, query)

	if !resultSlice.Ok {
		return NewResultT[map[string]T](resultSlice.Result)
	}

	result := make(map[string]T)

	for _, t := range resultSlice.Data {
		result[GetVID(t)] = t
	}

	return NewResultTWithData(resultSlice.Result, result)
}

func QueryVertexesByQueryToSlice[T interface{}](space *Space, query string) *ResultT[[]T] {
	r := space.Execute(CommandPipelineCombine(query, YieldVertexPropertyNamesCommand(utils.GetType[T]())))

	if !r.Ok {
		return NewResultT[[]T](r)
	}

	data := MappingResultToMap(r.DataSet)

	result := make([]T, 0)

	for _, rowData := range data {
		vertex := BuildNewVertexFromRowData[T](rowData)
		result = append(result, vertex)
	}

	return NewResultTWithData(r, result)
}

func GetAllVertexesVIDsByQuery[T interface{}](space *Space, query string) *ResultT[map[string]bool] {
	r := space.Execute(AllVertexesVidsByQueryCommand(utils.GetType[T](), query))

	if !r.Ok {
		return NewResultT[map[string]bool](r)
	}

	values, err := r.DataSet.GetValuesByColName("vid")

	if err != nil {
		return NewResultTWithError[map[string]bool](r, err)
	}

	result := make(map[string]bool)

	for _, value := range values {
		v, err := value.AsString()
		if err != nil {
			return NewResultTWithError[map[string]bool](r, err)
		}

		result[v] = true
	}

	return NewResultTWithData(r, result)
}

func GetAllVertexesPropertyByQuery[T interface{}](space *Space, query string, propertyName string, displayPropertyName string) *ResultT[map[string]bool] {
	if displayPropertyName == "" {
		displayPropertyName = propertyName
	}

	r := space.Execute(AllVertexesPropertyByQueryCommand(utils.GetType[T](), query, propertyName, displayPropertyName))

	if !r.Ok {
		return NewResultT[map[string]bool](r)
	}

	values, err := r.DataSet.GetValuesByColName(displayPropertyName)

	if err != nil {
		return NewResultTWithError[map[string]bool](r, err)
	}

	result := make(map[string]bool)

	for _, value := range values {
		v, err := value.AsString()
		if err != nil {
			return NewResultTWithError[map[string]bool](r, err)
		}

		result[v] = true
	}

	return NewResultTWithData(r, result)
}

func BuildNewVertexesFromResult[T interface{}](r *Result) []T {
	data := MappingResultToMap(r.DataSet)

	result := make([]T, len(data))

	for i, d := range data {
		ti := BuildNewVertexFromRowData[T](d)
		result[i] = ti
	}

	return result
}

func BuildNewVertexesReflectValuesFromResult(t reflect.Type, r *nebulago.ResultSet) map[string]reflect.Value {
	data := MappingResultToMap(r)

	result := make(map[string]reflect.Value)

	for _, d := range data {
		val := d["vid"].GetSVal()
		if len(val) == 0 {
			continue
		}

		v := reflect.New(t)
		LoadDataToVertexReflectValueFromRowDataMap(v, d)
		result[string(val)] = v.Elem()
	}

	return result
}

func BuildNewVertexFromResult[T interface{}](result *nebulago.ResultSet) T {
	var vertex T
	LoadDataToVertexReflectValueFromDataset(reflect.ValueOf(&vertex), result)

	return vertex
}

func BuildNewVertexFromRowData[T interface{}](rowData map[string]*nebulaggonebula.Value) T {
	var result T

	if len(rowData) > 0 {
		LoadDataToVertexReflectValueFromRowDataMap(reflect.ValueOf(&result), rowData)
	}

	return result
}

func IsVertex[T interface{}]() (bool, error) {
	hasTagName := false
	hasVidField := false

	typeOfVertex := utils.GetType[T]()

	for i := 0; i < typeOfVertex.NumField(); i++ {
		field := typeOfVertex.Field(i)

		if !hasTagName {
			tagName := field.Tag.Get("nebulatagname")
			if tagName != "" {
				hasTagName = true
			}
		}

		if !hasVidField {
			if field.Tag.Get("nebulakey") == "vid" {
				hasVidField = true
			}
		}

		if hasTagName && hasVidField {
			return true, nil
		}
	}

	var errorMessage []string
	if !hasTagName {
		errorMessage = append(errorMessage, "no tag name")
	}

	if !hasVidField {
		errorMessage = append(errorMessage, "no vid field")
	}

	return false, errors.New(strings.Join(errorMessage, ", "))
}

func GetVID(v interface{}) string {
	return getVIDByVertexReflectValue(reflect.ValueOf(v))
}

func getVIDByVertexReflectValue(v reflect.Value) string {
	valueOfVertex := utils.IndirectValue(v)
	typeOfVertex := valueOfVertex.Type()

	for i := 0; i < typeOfVertex.NumField(); i++ {
		fv := valueOfVertex.Field(i)
		ft := typeOfVertex.Field(i)

		if ft.Tag.Get("nebulakey") == "vid" {
			return fv.String()
		}
	}

	return ""
}

func getVertexInsertFieldAndValueString(v reflect.Value) (string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)
	vid := ""

	valueOfVertex := utils.IndirectValue(v)
	typeOfVertex := valueOfVertex.Type()

	for i := 0; i < typeOfVertex.NumField(); i++ {
		fv := valueOfVertex.Field(i)
		ft := typeOfVertex.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
			if isZeroValue(fv, ft) {
				propertiesValues = append(propertiesValues, getFieldValue(ft, fv))
			} else {
				propertiesValues = append(propertiesValues, getDefaultValue(ft, fv))
			}
		}

		if ft.Tag.Get("nebulakey") == "vid" {
			vid = fv.String()
		}
	}

	return strings.Join(propertiesNames, ", "), fmt.Sprintf("\"%s\":(%s)", vid, strings.Join(propertiesValues, ", "))
}

func getVertexUpdateFieldAndValueString(vv reflect.Value) (string, string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)
	vid := ""

	valueOfVertex := utils.IndirectValue(vv)
	typeOfVertex := valueOfVertex.Type()

	for i := 0; i < typeOfVertex.NumField(); i++ {
		fv := valueOfVertex.Field(i)
		ft := typeOfVertex.Field(i)
		property := ft.Tag.Get("nebulaproperty")
		if property != "" {
			if isZeroValue(fv, ft) {
				name := property + " AS " + property
				propertiesNames = append(propertiesNames, name)
				value := getFieldValue(ft, fv)
				propertiesValues = append(propertiesValues, property+" = "+value)
			}
		}

		if ft.Tag.Get("nebulakey") == "vid" {
			vid = fv.String()
		}
	}

	return vid, strings.Join(propertiesNames, ", "), strings.Join(propertiesValues, ", ")
}

func LoadDataToVertexReflectValueFromDataset(value reflect.Value, result *nebulago.ResultSet) {
	data := MappingResultToMap(result)

	if len(data) > 0 {
		LoadDataToVertexReflectValueFromRowDataMap(value, data[0])
	}
}

func LoadDataToVertexReflectValueFromRowDataMap(value reflect.Value, rowData map[string]*nebulaggonebula.Value) {
	v := utils.IndirectValue(value)
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		fv := v.Field(i)
		ft := t.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if rowData[tagProperty] != nil {
				MappingRowDataToPropertyValue(ft, fv, rowData[tagProperty])
			}
		}

		if ft.Tag.Get("nebulakey") == "vid" {
			fv.SetString(string(rowData["vid"].GetSVal()))
		}
	}
}
