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

func InsertEdges[T interface{}](space *Space, es ...T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	ok, err := IsEdge[T]()
	if !ok {
		return newErrorResult(err)
	}

	return space.Execute(edgeInsertCommand[T](es...))
}

func BatchInsertEdges[T interface{}](space *Space, batch int, es []T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	ok, err := IsEdge[T]()
	if !ok {
		return newErrorResult(err)
	}

	chunk := lo.Chunk(es, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := InsertEdges(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch insert %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
			return r
		}
	}

	return newSuccessResult(cmds...)
}

func UpdateEdges[T interface{}](space *Space, es ...T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	commands := make([]string, len(es))
	for i, t := range es {
		commands[i] = edgeUpdateCommand(t)
	}

	return space.Execute(commands...)
}

func BatchUpdateEdges[T interface{}](space *Space, batch int, es []T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	chunk := lo.Chunk(es, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := UpdateEdges(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch update %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return newSuccessResult(cmds...)
}

func UpsertEdges[T interface{}](space *Space, es ...T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	commands := make([]string, len(es))
	for i, t := range es {
		commands[i] = edgeUpsertCommand(t)
	}

	return space.Execute(commands...)
}

func BatchUpsertEdges[T interface{}](space *Space, batch int, es []T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	chunk := lo.Chunk(es, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := UpsertEdges(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch upsert %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, r.Err.Error()))
			return r
		}
	}

	return newSuccessResult(cmds...)
}

func DeleteEdges[T interface{}](space *Space, es ...T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	eids := make([]*EID, len(es))
	for i, e := range es {
		eids[i] = GetEIDByEdge(e)
	}

	return space.Execute(edgeDeleteByEidsCommand(eids...))
}

func BatchDeleteEdges[T interface{}](space *Space, batch int, es []T) *Result {
	if len(es) == 0 {
		return newErrorResult(errors.New("no edges"))
	}

	ok, err := IsEdge[T]()
	if !ok {
		return newErrorResult(err)
	}

	chunk := lo.Chunk(es, batch)

	cmds := make([]string, 0)
	for i, c := range chunk {
		r := DeleteEdges(space, c...)
		cmds = append(cmds, r.Commands...)

		if !r.Ok {
			r.Err = errors.New(fmt.Sprintf("batch delete %d edges from %d to %d failed: %s", i, i*batch, len(c)-1, err.Error()))
			return r
		}
	}

	return newSuccessResult(cmds...)
}

func DeleteEdgesByFromIdAndToId[T interface{}](space *Space, fromId string, toId string) *Result {
	return space.Execute(edgeDeleteByEidsCommand(NewEID(fromId, toId, GetEdgeName[T]())))
}

func DeleteEdgesByEids(space *Space, eids ...*EID) *Result {
	if len(eids) == 0 {
		return newErrorResult(errors.New("no edge ids"))
	}

	return space.Execute(edgeDeleteByEidsCommand(eids...))
}

func DeleteAllEdgesByEdgeType[T interface{}](space *Space) *Result {
	return DeleteAllEdgesByQuery[T](space, "")
}

func DeleteAllEdgesByQuery[T interface{}](space *Space, query string) *Result {
	return DeleteEdgesByQuery[T](space, AllEdgesFromVidsAndToVidsByQueryCommand(utils.GetType[T](), query))
}

func DeleteEdgesByQuery[T interface{}](space *Space, query string) *Result {
	return space.Execute(edgesDeleteByQueryCommand(utils.GetType[T](), query))
}

func LoadEdge[T interface{}](space *Space, e T) *Result {
	er, fr, tr := FetchEdgeData[T](space, GetEIDByEdge(e))

	r := checkEdgeSearchResult(er, fr, tr)

	if !r.Ok {
		return r
	}

	LoadDataToEdgeReflectValueFromDataset(reflect.ValueOf(e), er.DataSet, fr.Data, tr.Data)

	return r
}

func GetAllEdgesEIDsByQuery[T interface{}](space *Space, query string) *ResultT[map[string]bool] {
	r := space.Execute(AllEdgesFromVidsAndToVidsByQueryCommand(utils.GetType[T](), query))

	if !r.Ok {
		return newResultT[map[string]bool](r)
	}

	srcValues, err := r.DataSet.GetValuesByColName("src")

	if err != nil {
		return newResultTWithError[map[string]bool](r, err)
	}

	dstValues, err := r.DataSet.GetValuesByColName("dst")

	if err != nil {
		return newResultTWithError[map[string]bool](r, err)
	}

	result := make(map[string]bool)

	for i, value := range srcValues {
		src, err := value.AsString()
		if err != nil {
			return newResultTWithError[map[string]bool](r, err)
		}

		dst, err := dstValues[i].AsString()
		if err != nil {
			return newResultTWithError[map[string]bool](r, err)
		}

		result[fmt.Sprintf("\"%s\"->\"%s\"", src, dst)] = true
	}

	return newResultTWithData(r, result)
}

func GetAllEdgesByEdgeType[T interface{}](space *Space) *ResultT[map[string]T] {
	return GetAllEdgesByQuery[T](space, "")
}

func GetAllEdgesByQuery[T interface{}](space *Space, query string) *ResultT[map[string]T] {
	return GetEdgesByQuery[T](space, LookupEdgeQueryCommand(utils.GetType[T](), query))
}

func GetEdgesByQuery[T interface{}](space *Space, query string) *ResultT[map[string]T] {
	er, fr, tr := QueryByEdgeQuery[T](space, query)

	r := checkEdgeSearchResult(er, fr, tr)

	result := BuildEdgesFromResult[T](er.DataSet, fr.Data, tr.Data)

	return newResultTWithData(r, result)
}

func GetEdgeByEid[T interface{}](space *Space, eid *EID) *ResultT[T] {
	er, fr, tr := FetchEdgeData[T](space, eid)

	r := checkEdgeSearchResult(er, fr, tr)

	if !r.Ok {
		return newResultT[T](r)
	}

	data := BuildNewEdgeFromResult[T](er.DataSet, fr.Data, tr.Data)

	return newResultTWithData(r, data)
}

func FetchEdgeData[T interface{}](space *Space, eid *EID) (*Result, *ResultT[map[string]reflect.Value], *ResultT[map[string]reflect.Value]) {
	return QueryByEdgeQuery[T](space, FetchEdgeQueryCommand(eid))
}

func QueryByEdgeQuery[T interface{}](space *Space, edgeQuery string) (*Result, *ResultT[map[string]reflect.Value], *ResultT[map[string]reflect.Value]) {
	t := utils.GetType[T]()
	cmd := QueryByEdgeQueryCommand(t, edgeQuery)
	edgeResult := space.Execute(cmd)

	ft, tt := getEdgeFromAndToType(t)

	fr := QueryByVertexQuery(space, ft, CommandPipelineCombine(cmd, DistinctFetchVertexByQueryCommand(ft, "$-.src")))
	if !fr.Ok {
		return edgeResult, newResultT[map[string]reflect.Value](fr), newErrorResultT[map[string]reflect.Value](errors.New("haven't query to vertexes"))
	}
	fromData := BuildNewVertexesReflectValuesFromResult(ft, fr.DataSet)
	fromResult := newResultTWithData(fr, fromData)

	tr := QueryByVertexQuery(space, tt, CommandPipelineCombine(cmd, DistinctFetchVertexByQueryCommand(tt, "$-.dst")))
	if !tr.Ok {
		return edgeResult, fromResult, newResultT[map[string]reflect.Value](tr)
	}
	toData := BuildNewVertexesReflectValuesFromResult(tt, tr.DataSet)
	toResult := newResultTWithData(tr, toData)

	return edgeResult, fromResult, toResult
}

func BuildEdgesFromResult[T interface{}](edgeResult *nebulago.ResultSet, fromResult map[string]reflect.Value, toResult map[string]reflect.Value) map[string]T {
	result := make(map[string]T)

	edgeData := MappingResultToMap(edgeResult)

	for _, rowData := range edgeData {
		var e T
		LoadDataToEdgeReflectValueFromRowDataMap(reflect.ValueOf(&e), rowData, fromResult, toResult)
		result[GetEIDByEdge(e).String()] = e
	}

	return result
}

func BuildNewEdgeFromResult[T interface{}](edgeResult *nebulago.ResultSet, fromResult map[string]reflect.Value, toResult map[string]reflect.Value) T {
	var vertex T
	LoadDataToEdgeReflectValueFromDataset(reflect.ValueOf(&vertex), edgeResult, fromResult, toResult)

	return vertex
}

func IsEdge[T interface{}]() (bool, error) {
	hasEdgeName := false
	hasFromField := false
	hasToField := false

	typeOfTag := utils.GetType[T]()

	for i := 0; i < typeOfTag.NumField(); i++ {
		field := typeOfTag.Field(i)

		if !hasEdgeName {
			edgeName := field.Tag.Get("nebulaedgename")
			if edgeName != "" {
				hasEdgeName = true
			}
		}

		if !hasFromField {
			if field.Tag.Get("nebulakey") == "edgefrom" {
				hasFromField = true
			}
		}

		if !hasToField {
			if field.Tag.Get("nebulakey") == "edgeto" {
				hasToField = true
			}
		}

		if hasEdgeName && hasFromField && hasToField {
			return true, nil
		}
	}

	var errorMessage []string

	if !hasEdgeName {
		errorMessage = append(errorMessage, "no edge name")
	}

	if !hasFromField {
		errorMessage = append(errorMessage, "no edge from field")
	}

	if !hasToField {
		errorMessage = append(errorMessage, "no edge to field")
	}

	return false, errors.New(strings.Join(errorMessage, ", "))
}

func GetEdgeName[T interface{}]() string {
	return getEdgeNameByReflectType(utils.GetType[T]())
}

func getEdgeNameByReflectType(t reflect.Type) string {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		edgeName := field.Tag.Get("nebulaedgename")
		if edgeName != "" {
			return edgeName
		}
	}

	return ""
}

func getEdgeInsertFieldAndValueString(ev reflect.Value) (string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)
	from := ""
	to := ""

	valueOfEdge := utils.IndirectValue(ev)
	typeOfEdge := valueOfEdge.Type()

	for i := 0; i < typeOfEdge.NumField(); i++ {
		fv := valueOfEdge.Field(i)
		ft := typeOfEdge.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			propertiesNames = append(propertiesNames, tagProperty)
			if isZeroValue(fv, ft) {
				propertiesValues = append(propertiesValues, getFieldValue(ft, fv))
			} else {
				propertiesValues = append(propertiesValues, getDefaultValue(ft, fv))
			}
		}

		if ft.Tag.Get("nebulakey") == "edgefrom" {
			from = getVIDByVertexReflectValue(fv)
		}

		if ft.Tag.Get("nebulakey") == "edgeto" {
			to = getVIDByVertexReflectValue(fv)
		}
	}

	return strings.Join(propertiesNames, ", "), fmt.Sprintf("\"%s\"->\"%s\":(%s)", from, to, strings.Join(propertiesValues, ", "))
}

func getEdgeFromAndToType(t reflect.Type) (reflect.Type, reflect.Type) {
	var from reflect.Type
	var to reflect.Type

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)

		if ft.Tag.Get("nebulakey") == "edgefrom" {
			from = utils.IndirectValue(reflect.New(ft.Type)).Type()
		}

		if ft.Tag.Get("nebulakey") == "edgeto" {
			to = utils.IndirectValue(reflect.New(ft.Type)).Type()
		}

		if from != nil && to != nil {
			break
		}
	}

	return from, to
}

func getEdgeUpdateFieldAndValueString(ev reflect.Value) (string, string, string) {
	propertiesValues := make([]string, 0)
	propertiesNames := make([]string, 0)
	from := ""
	to := ""

	valueOfEdge := utils.IndirectValue(ev)
	typeOfEdge := valueOfEdge.Type()

	for i := 0; i < typeOfEdge.NumField(); i++ {
		fv := valueOfEdge.Field(i)
		ft := typeOfEdge.Field(i)
		property := ft.Tag.Get("nebulaproperty")
		if property != "" {
			if isZeroValue(fv, ft) {
				name := property + " AS " + property
				propertiesNames = append(propertiesNames, name)
				value := getFieldValue(ft, fv)
				propertiesValues = append(propertiesValues, property+" = "+value)
			}
		}

		if ft.Tag.Get("nebulakey") == "edgefrom" {
			from = getVIDByVertexReflectValue(fv)
		}

		if ft.Tag.Get("nebulakey") == "edgeto" {
			to = getVIDByVertexReflectValue(fv)
		}
	}

	return fmt.Sprintf("\"%s\"->\"%s\"", from, to), strings.Join(propertiesNames, ", "), strings.Join(propertiesValues, ", ")
}

func LoadDataToEdgeReflectValueFromDataset(value reflect.Value, edgeResult *nebulago.ResultSet, fromResult map[string]reflect.Value, toResult map[string]reflect.Value) {
	edgeData := MappingResultToMap(edgeResult)

	if len(edgeData) > 0 {
		LoadDataToEdgeReflectValueFromRowDataMap(value, edgeData[0], fromResult, toResult)
	}
}

func LoadDataToEdgeReflectValueFromRowDataMap(value reflect.Value, edgeRowData map[string]*nebulaggonebula.Value, fromResult map[string]reflect.Value, toResult map[string]reflect.Value) {
	v := utils.IndirectValue(value)
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		fv := v.Field(i)
		ft := t.Field(i)
		tagProperty := ft.Tag.Get("nebulaproperty")
		if tagProperty != "" {
			if edgeRowData[tagProperty] != nil {
				MappingRowDataToPropertyValue(ft, fv, edgeRowData[tagProperty])
			}
		}

		if ft.Tag.Get("nebulakey") == "edgefrom" {
			if d, ok := edgeRowData["src"]; ok {
				fk := string(d.GetSVal())
				fvv := utils.IndirectValue(fv)
				if fkv, ok := fromResult[fk]; ok {
					fvv.Set(fkv)
				} else {
					dd := make(map[string]*nebulaggonebula.Value)
					ddv := nebulaggonebula.Value{}
					ddv.SetSVal([]byte(fk))
					dd["vid"] = &ddv
					LoadDataToVertexReflectValueFromRowDataMap(fvv, dd)
					fromResult[fk] = fvv
				}
			}
		}

		if ft.Tag.Get("nebulakey") == "edgeto" {
			if d, ok := edgeRowData["dst"]; ok {
				fk := string(d.GetSVal())
				fvv := utils.IndirectValue(fv)
				if fkv, ok := toResult[fk]; ok {
					fvv.Set(fkv)
				} else {
					dd := make(map[string]*nebulaggonebula.Value)
					ddv := nebulaggonebula.Value{}
					ddv.SetSVal([]byte(fk))
					dd["vid"] = &ddv
					LoadDataToVertexReflectValueFromRowDataMap(fvv, dd)
					toResult[fk] = fvv
				}
			}
		}
	}
}

func checkEdgeSearchResult(er *Result, fr *ResultT[map[string]reflect.Value], tr *ResultT[map[string]reflect.Value]) *Result {
	if !er.Ok {
		return er
	}

	if !fr.Ok {
		return fr.Result
	}

	if !tr.Ok {
		return tr.Result
	}

	return er
}
