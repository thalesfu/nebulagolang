package nebulagolang

import (
	"fmt"
	"reflect"
	"strings"
)

const YieldEdgeFromVidToVidCommand = "YIELD src($-.e) AS src, dst($-.e) AS dst"

func AllEdgesFromVidsAndToVidsByQueryCommand(t reflect.Type, query string) string {
	return CommandPipelineCombine(LookupEdgeQueryCommand(t, query), YieldEdgeFromVidToVidCommand)
}
func LookupEdgeQueryCommand(t reflect.Type, query string) string {
	edgeName := getEdgeNameByReflectType(t)
	if query == "" {
		return fmt.Sprintf("LOOKUP ON %s YIELD edge AS e", edgeName)
	}

	return fmt.Sprintf("LOOKUP ON %s WHERE %s YIELD edge AS e", edgeName, query)
}

func YieldEdgePropertyNamesCommand(t reflect.Type) string {
	pns := GetPropertiesNames(t)

	commands := make([]string, len(pns)+1)
	commands[0] = YieldEdgeFromVidToVidCommand
	for i, pn := range pns {
		commands[i+1] = "properties($-.e)." + pn + " AS " + pn
	}

	return strings.Join(commands, ", ")
}

func AllEdgesByQueryCommand(t reflect.Type, query string) string {
	return QueryByEdgeQueryCommand(t, LookupEdgeQueryCommand(t, query))
}

func QueryByEdgeQueryCommand(t reflect.Type, edgeQuery string) string {
	return CommandPipelineCombine(edgeQuery, YieldEdgePropertyNamesCommand(t))
}

func FetchEdgeQueryCommand(eid *EID) string {
	return fmt.Sprintf("FETCH PROP ON %s %s YIELD EDGE AS e", eid.edgeName, eid.String())
}
