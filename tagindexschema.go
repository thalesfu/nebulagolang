package nebulagolang

import (
	"fmt"
	"strings"
)

type TagIndexSchema struct {
	Name       string
	TagName    string
	Properties []*TagPropertySchema
}

func NewTagIndexSchema(tagName string, properties ...*TagPropertySchema) *TagIndexSchema {
	index := &TagIndexSchema{
		TagName:    tagName,
		Properties: properties,
	}

	if len(properties) == 0 {
		index.Name = getTagIndexPrefix(tagName)
	} else {
		pn := make([]string, len(properties))
		for i, prop := range properties {
			pn[i] = prop.Name
		}

		index.Name = fmt.Sprintf("%s_%s", getTagIndexPrefix(tagName), strings.Join(pn, "_"))
	}

	return index
}

func getTagIndexPrefix(tagName string) string {
	return fmt.Sprintf("tag_index_%s", tagName)
}

func (ti *TagIndexSchema) CreateIndexString() string {
	indexNames := make([]string, len(ti.Properties))
	for i, prop := range ti.Properties {
		indexNames[i] = prop.IndexName()
	}

	builder := strings.Builder{}
	builder.WriteString("CREATE TAG INDEX IF NOT EXISTS ")
	builder.WriteString(ti.Name)
	builder.WriteString(" ON ")
	builder.WriteString(ti.TagName)
	builder.WriteString("(")
	builder.WriteString(strings.Join(indexNames, ", "))
	builder.WriteString(");")
	return builder.String()
}
