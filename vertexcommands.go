package nebulagolang

import (
	"fmt"
	"reflect"
	"strings"
)

func vertexInsertCommand[T interface{}](vs ...T) string {
	pns, pvs := make([]string, len(vs)), make([]string, len(vs))

	for i, v := range vs {
		pn, pv := getVertexInsertFieldAndValueString(reflect.ValueOf(v))
		pns[i] = pn
		pvs[i] = pv
	}

	return fmt.Sprintf("INSERT VERTEX IF NOT EXISTS %s(%s) VALUES %s", GetTagName[T](), pns[0], strings.Join(pvs, ", "))
}

func vertexUpdateCommand[T interface{}](v T) string {
	vid, pns, pvs := getVertexUpdateFieldAndValueString(reflect.ValueOf(v))
	return fmt.Sprintf("UPDATE VERTEX ON %s \"%s\" SET %s YIELD %s", GetTagName[T](), vid, pvs, pns)
}

func vertexUpsertCommand[T interface{}](v T) string {
	vid, pns, pvs := getVertexUpdateFieldAndValueString(reflect.ValueOf(v))
	return fmt.Sprintf("UPSERT VERTEX ON %s \"%s\" SET %s YIELD %s", GetTagName[T](), vid, pvs, pns)
}

func vertexDeleteByVertexesVidsCommand[T interface{}](vs ...T) string {
	vids := make([]string, len(vs))
	for i, v := range vs {
		vids[i] = GetVID(v)
	}

	return vertexDeleteByVidsCommand(vids...)
}

func vertexDeleteByVidsCommand(vids ...string) string {
	vs := make([]string, len(vids))

	for i, v := range vids {
		vs[i] = fmt.Sprintf("\"%s\"", v)
	}
	return fmt.Sprintf("DELETE VERTEX %s", strings.Join(vs, ", "))
}

func vertexDeleteWithEdgeByVertexesVidsCommand[T interface{}](vs ...T) string {
	vids := make([]string, len(vs))
	for i, v := range vs {
		vids[i] = GetVID(v)
	}

	return vertexDeleteWithEdgeByVidsCommand(vids...)
}

func vertexDeleteWithEdgeByVidsCommand(vids ...string) string {
	vs := make([]string, len(vids))

	for i, v := range vids {
		vs[i] = fmt.Sprintf("\"%s\"", v)
	}

	return fmt.Sprintf("DELETE VERTEX %s WITH EDGE", strings.Join(vs, ", "))
}

const PipelineDeleteVertexByVidCommand = "DELETE VERTEX $-.vid"
const PipelineDeleteVertexWithEdgeByVidCommand = "DELETE VERTEX $-.vid WITH EDGE"

func vertexesDeleteByQueryCommand(query string) string {
	return CommandPipelineCombine(query, PipelineDeleteVertexByVidCommand)
}

func vertexesDeleteWithEdgeByQueryCommand(query string) string {
	return CommandPipelineCombine(query, PipelineDeleteVertexWithEdgeByVidCommand)
}
