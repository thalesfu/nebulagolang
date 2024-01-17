package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/nebulagolang/basictype"
	"github.com/thalesfu/nebulagolang/utils"
	"reflect"
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

func BuildEdgeSchema[T interface{}]() (*EdgeSchema, bool) {
	typeOfEdge := utils.GetType[T]()
	edgeSchema, ok := generateEdgeSchema(typeOfEdge)

	if !ok {
		return nil, false
	}

	return edgeSchema, true
}

func generateEdgeSchema(t reflect.Type) (*EdgeSchema, bool) {
	edgeName := ""
	edgeComment := ""

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		name := ft.Tag.Get("nebulaedgename")
		if name != "" {
			edgeName = name
		}

		comment := ft.Tag.Get("nebulaedgecomment")
		if comment != "" {
			edgeComment = comment
		}
	}

	if edgeName != "" {
		edgeSchema := NewEdgeSchema(edgeName)
		edgeSchema.Comment = edgeComment

		properties, indexes := generateEdgePropertiesAndIndexes(t)

		for _, prop := range properties {
			edgeSchema.AddProperty(prop)
		}

		edgeSchema.AddIndex()
		for _, index := range indexes {
			edgeSchema.AddIndex(index...)
		}

		return edgeSchema, true
	}

	return nil, false
}

func generateEdgePropertiesAndIndexes(t reflect.Type) ([]*EdgePropertySchema, map[string][]*EdgePropertySchema) {
	properties := make([]*EdgePropertySchema, 0)
	indexes := make(map[string][]*EdgePropertySchema)

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		propertyName := ft.Tag.Get("nebulaproperty")

		if propertyName == "" {
			continue
		}

		propertyType := basictype.GetTypeByReflectFieldStruct(ft)

		schema := NewEdgePropertySchema(propertyName, propertyType)

		comment := ft.Tag.Get("description")
		if comment != "" {
			schema.Comment = comment
		}

		properties = append(properties, schema)

		idxstring := ft.Tag.Get("nebulaindexes")

		idxes := strings.Split(idxstring, ",")

		for _, idx := range idxes {
			if _, ok := indexes[idx]; !ok {
				indexes[idx] = make([]*EdgePropertySchema, 0)
				indexes[idx] = append(indexes[idx], schema)
			} else {
				indexes[idx] = append(indexes[idx], schema)
			}
		}
	}

	return properties, indexes
}
