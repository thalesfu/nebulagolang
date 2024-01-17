package nebulagolang

import (
	"fmt"
	"reflect"
	"strings"
)

const YieldVertexVidCommand = "YIELD id($-.v) AS vid"

func LookupTagQueryCommand(t reflect.Type, query string) string {
	if query == "" {
		return fmt.Sprintf("LOOKUP ON %s YIELD VERTEX AS v", getTagNameByReflectType(t))
	}

	return fmt.Sprintf("LOOKUP ON %s WHERE %s YIELD VERTEX AS v", getTagNameByReflectType(t), query)
}

func YieldVertexPropertyNamesCommand(t reflect.Type) string {
	pns := GetPropertiesNames(t)
	for i, pn := range pns {
		pns[i] = "properties($-.v)." + pn + " AS " + pn
	}

	return fmt.Sprintf("%s, %s", YieldVertexVidCommand, strings.Join(pns, ", "))
}

func QueryByVertexQueryCommand(t reflect.Type, tagQuery string) string {
	return CommandPipelineCombine(tagQuery, YieldVertexPropertyNamesCommand(t))
}

func FetchVertexByVidCommand(t reflect.Type, vid string) string {
	return fmt.Sprintf("FETCH PROP ON %s \"%s\" YIELD VERTEX AS v", getTagNameByReflectType(t), vid)
}

func DistinctFetchVertexByQueryCommand(t reflect.Type, query string) string {
	return fmt.Sprintf("FETCH PROP ON %s %s YIELD DISTINCT VERTEX AS v", getTagNameByReflectType(t), query)
}

func AllVertexesByQueryCommand(t reflect.Type, query string) string {
	return CommandPipelineCombine(LookupTagQueryCommand(t, query), YieldVertexPropertyNamesCommand(t))
}

func AllVertexesVidsByQueryCommand(t reflect.Type, query string) string {
	return CommandPipelineCombine(LookupTagQueryCommand(t, query), YieldVertexVidCommand)
}

func AllVertexesPropertyByQueryCommand(t reflect.Type, query string, propertyName string, displayPropertyName string) string {
	return CommandPipelineCombine(LookupTagQueryCommand(t, query), fmt.Sprintf("YIELD properties($-.v).%s AS %s", propertyName, displayPropertyName))
}
