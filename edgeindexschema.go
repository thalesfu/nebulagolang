package nebulagolang

import (
	"fmt"
	"strings"
)

type EdgeIndexSchema struct {
	Name       string
	edgeName   string
	Properties []*EdgePropertySchema
}

func NewEdgeIndexSchema(edgeName string, properties ...*EdgePropertySchema) *EdgeIndexSchema {
	index := &EdgeIndexSchema{
		edgeName:   edgeName,
		Properties: properties,
	}

	if len(properties) == 0 {
		index.Name = getEdgeIndexPrefix(edgeName)
	} else {
		pn := make([]string, len(properties))
		for i, prop := range properties {
			pn[i] = prop.Name
		}

		index.Name = fmt.Sprintf("%s_%s", getEdgeIndexPrefix(edgeName), strings.Join(pn, "_"))
	}

	return index
}

func getEdgeIndexPrefix(edgeName string) string {
	return fmt.Sprintf("edge_index_%s", edgeName)
}

func (eis *EdgeIndexSchema) CreateIndexString() string {
	indexNames := make([]string, len(eis.Properties))
	for i, prop := range eis.Properties {
		indexNames[i] = prop.IndexName()
	}

	builder := strings.Builder{}
	builder.WriteString("CREATE EDGE INDEX IF NOT EXISTS ")
	builder.WriteString(eis.Name)
	builder.WriteString(" ON ")
	builder.WriteString(eis.edgeName)
	builder.WriteString("(")
	builder.WriteString(strings.Join(indexNames, ", "))
	builder.WriteString(");")
	return builder.String()
}
