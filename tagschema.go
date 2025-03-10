package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/golangutils"
	"github.com/thalesfu/nebulagolang/basictype"
	"reflect"
	"strings"
	"time"
)

type TagSchema struct {
	Name        string                        `yaml:"name"`
	Properties  map[string]*TagPropertySchema `yaml:"properties"`
	TTLDuration time.Duration                 `yaml:"ttl_duration"`
	Comment     string                        `yaml:"comment"`
	Indexes     map[string]*TagIndexSchema    `yaml:"indexes"`
}

func NewTagSchema(name string) *TagSchema {
	return &TagSchema{
		Name:       name,
		Properties: make(map[string]*TagPropertySchema),
		Indexes:    make(map[string]*TagIndexSchema),
	}
}

func (s *TagSchema) AddProperty(prop *TagPropertySchema) {
	s.Properties[prop.Name] = prop
}

func (s *TagSchema) AddIndex(properties ...*TagPropertySchema) *TagIndexSchema {
	indexSchema := NewTagIndexSchema(s.Name, properties...)
	s.Indexes[indexSchema.Name] = indexSchema
	return indexSchema
}

func (s *TagSchema) PropertiesString() string {
	builder := strings.Builder{}
	for _, prop := range s.Properties {
		builder.WriteString(prop.String())
		builder.WriteString(", ")
	}

	return builder.String()
}

func (s *TagSchema) CreateString() string {

	builder := strings.Builder{}
	builder.WriteString("CREATE TAG IF NOT EXISTS ")
	builder.WriteString(s.Name)
	builder.WriteString("(")
	builder.WriteString(s.PropertiesString())
	builder.WriteString(")")

	additionalCommand := make([]string, 0)

	if s.TTLDuration != 0 {
		additionalCommand = append(additionalCommand, fmt.Sprintf("TTL_DURATION = %s", s.TTLDuration.String()))
	}

	for _, prop := range s.Properties {
		if prop.IsTTLColumn {
			additionalCommand = append(additionalCommand, fmt.Sprintf("TTL_COL = \"%s\"", prop.Name))
			break
		}
	}

	if s.Comment != "" {
		additionalCommand = append(additionalCommand, fmt.Sprintf("COMMENT = \"%s\"", s.Comment))
	}

	return builder.String() + strings.Join(additionalCommand, ", ") + ";"
}

func BuildTagSchema[T interface{}]() (*TagSchema, bool) {
	typeOfTag := golangutils.GetType[T]()
	tagSchema, ok := generateTagSchema(typeOfTag)

	if !ok {
		return nil, false
	}

	return tagSchema, true
}

func generateTagSchema(t reflect.Type) (*TagSchema, bool) {
	tagName := ""
	tagComment := ""

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		name := ft.Tag.Get("nebulatagname")
		if name != "" {
			tagName = name
		}

		comment := ft.Tag.Get("nebulatagcomment")
		if comment != "" {
			tagComment = comment
		}
	}

	if tagName != "" {
		tagSchema := NewTagSchema(tagName)
		tagSchema.Comment = tagComment

		properties, indexes := generateTagPropertiesAndIndexes(t)

		for _, prop := range properties {
			tagSchema.AddProperty(prop)
		}

		for _, index := range indexes {
			tagSchema.AddIndex(index...)
		}

		tagSchema.AddIndex()

		return tagSchema, true
	}

	return nil, false
}

func generateTagPropertiesAndIndexes(t reflect.Type) ([]*TagPropertySchema, map[string][]*TagPropertySchema) {
	properties := make([]*TagPropertySchema, 0)
	indexes := make(map[string][]*TagPropertySchema)

	for i := 0; i < t.NumField(); i++ {
		ft := t.Field(i)
		propertyName := ft.Tag.Get("nebulaproperty")

		if propertyName == "" {
			continue
		}

		propertyType := basictype.GetTypeByReflectFieldStruct(ft)

		schema := NewTagPropertySchema(propertyName, propertyType)

		comment := ft.Tag.Get("description")
		if comment != "" {
			schema.Comment = comment
		}

		properties = append(properties, schema)

		idxstring := ft.Tag.Get("nebulaindexes")

		idxes := strings.Split(idxstring, ",")

		for _, idx := range idxes {
			if _, ok := indexes[idx]; !ok {
				indexes[idx] = make([]*TagPropertySchema, 0)
				indexes[idx] = append(indexes[idx], schema)
			} else {
				indexes[idx] = append(indexes[idx], schema)
			}
		}
	}

	return properties, indexes
}
