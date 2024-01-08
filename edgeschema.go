package nebulagolang

import (
	"fmt"
	"strings"
	"time"
)

type EdgeSchema struct {
	Name        string                         `yaml:"name"`
	Properties  map[string]*EdgePropertySchema `yaml:"properties"`
	TTLDuration time.Duration                  `yaml:"ttl_duration"`
	Comment     string                         `yaml:"comment"`
	Indexes     map[string]*EdgeIndexSchema    `yaml:"indexes"`
}

func NewEdgeSchema(name string) *EdgeSchema {
	return &EdgeSchema{
		Name:       name,
		Properties: make(map[string]*EdgePropertySchema),
		Indexes:    make(map[string]*EdgeIndexSchema),
	}
}

func (es *EdgeSchema) AddProperty(prop *EdgePropertySchema) {
	es.Properties[prop.Name] = prop
}

func (es *EdgeSchema) AddIndex(properties ...*EdgePropertySchema) *EdgeIndexSchema {
	indexSchema := NewEdgeIndexSchema(es.Name, properties...)
	es.Indexes[indexSchema.Name] = indexSchema
	return indexSchema
}

func (es *EdgeSchema) PropertiesString() string {
	builder := strings.Builder{}
	for _, prop := range es.Properties {
		builder.WriteString(prop.String())
		builder.WriteString(", ")
	}

	return builder.String()
}

func (es *EdgeSchema) CreateString() string {

	builder := strings.Builder{}
	builder.WriteString("CREATE EDGE IF NOT EXISTS ")
	builder.WriteString(es.Name)
	builder.WriteString("(")
	builder.WriteString(es.PropertiesString())
	builder.WriteString(")")

	additionalCommand := make([]string, 0)

	if es.TTLDuration != 0 {
		additionalCommand = append(additionalCommand, fmt.Sprintf("TTL_DURATION = %s", es.TTLDuration.String()))
	}

	for _, prop := range es.Properties {
		if prop.IsTTLColumn {
			additionalCommand = append(additionalCommand, fmt.Sprintf("TTL_COL = \"%s\"", prop.Name))
			break
		}
	}

	if es.Comment != "" {
		additionalCommand = append(additionalCommand, fmt.Sprintf("COMMENT = \"%s\"", es.Comment))
	}

	return builder.String() + strings.Join(additionalCommand, ", ") + ";"
}
