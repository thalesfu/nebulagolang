package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/nebulagolang/basictype"
	"github.com/thalesfu/nebulagolang/nullable"
	"strings"
)

type TagPropertySchema struct {
	Name        string
	Type        basictype.BasicType
	Nullable    nullable.Nullable
	Default     any
	Comment     string
	IsTTLColumn bool
}

func NewTagPropertySchema(name string, t basictype.BasicType) *TagPropertySchema {
	return &TagPropertySchema{
		Name: name,
		Type: t,
	}
}

func (s *TagPropertySchema) String() string {
	builder := strings.Builder{}
	builder.WriteString(s.Name)
	builder.WriteString(" ")
	builder.WriteString(s.Type.String())
	if s.Nullable != "" {
		builder.WriteString(" ")
		builder.WriteString(s.Nullable)
	}

	if s.Default != nil {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(fmt.Sprintf("%v", s.Default))
	}

	if s.Comment != "" {
		builder.WriteString(" COMMENT ")
		builder.WriteString(fmt.Sprintf("'%s'", s.Comment))
	}

	return builder.String()
}

func (s *TagPropertySchema) IndexName() string {
	if s.Type.Name == "STRING" {
		return fmt.Sprintf("%s(%d)", s.Name, s.Type.IndexLength)
	} else {
		return s.Name
	}
}
