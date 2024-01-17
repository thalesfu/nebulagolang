package nebulagolang

import (
	"fmt"
	"reflect"
	"strings"
)

func edgeInsertCommand[T interface{}](es ...T) string {
	pns, pvs := make([]string, len(es)), make([]string, len(es))

	for i, e := range es {
		pn, pv := getEdgeInsertFieldAndValueString(reflect.ValueOf(e))
		pns[i] = pn
		pvs[i] = pv
	}

	return fmt.Sprintf("INSERT EDGE IF NOT EXISTS %s(%s) VALUES %s", GetEdgeName[T](), pns[0], strings.Join(pvs, ", "))
}

func edgeUpdateCommand[T interface{}](e T) string {
	eid, pns, pvs := getEdgeUpdateFieldAndValueString(reflect.ValueOf(e))

	return fmt.Sprintf("UPDATE EDGE ON %s %s SET %s YIELD %s", GetEdgeName[T](), eid, pvs, pns)
}

func edgeUpsertCommand[T interface{}](e T) string {
	eid, pns, pvs := getEdgeUpdateFieldAndValueString(reflect.ValueOf(e))

	return fmt.Sprintf("UPSERT EDGE ON %s %s SET %s YIELD %s", GetEdgeName[T](), eid, pvs, pns)
}

func edgeDeleteByEidsCommand(eids ...*EID) string {
	es := make([]string, len(eids))

	for i, e := range eids {
		es[i] = e.String()
	}
	return fmt.Sprintf("DELETE EDGE %s %s", eids[0].Type(), strings.Join(es, ", "))
}

func pipelineDeleteEdgeByFromVidAndToVid(t reflect.Type) string {
	return fmt.Sprintf("DELETE edge %s $-.src -> $-.dst", getEdgeNameByReflectType(t))
}

func edgesDeleteByQueryCommand(t reflect.Type, query string) string {
	return CommandPipelineCombine(query, pipelineDeleteEdgeByFromVidAndToVid(t))
}
