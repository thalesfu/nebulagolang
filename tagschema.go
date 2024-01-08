package nebulagolang

import (
	"fmt"
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
