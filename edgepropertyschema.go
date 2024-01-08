package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/nebulagolang/basictype"
	"github.com/thalesfu/nebulagolang/nullable"
	"strings"
)

type EdgePropertySchema struct {
	Name        string
	Type        basictype.BasicType
	Nullable    nullable.Nullable
	Default     any
	Comment     string
	IsTTLColumn bool
}

func NewEdgePropertySchema(name string, t basictype.BasicType) *EdgePropertySchema {
	return &EdgePropertySchema{
		Name: name,
		Type: t,
	}
}

func (eps *EdgePropertySchema) String() string {
	builder := strings.Builder{}
	builder.WriteString(eps.Name)
	builder.WriteString(" ")
	builder.WriteString(eps.Type.String())
	if eps.Nullable != "" {
		builder.WriteString(" ")
		builder.WriteString(eps.Nullable)
	}

	if eps.Default != nil {
		builder.WriteString(" DEFAULT ")
		builder.WriteString(fmt.Sprintf("%v", eps.Default))
	}

	if eps.Comment != "" {
		builder.WriteString(" COMMENT ")
		builder.WriteString(fmt.Sprintf("'%s'", eps.Comment))
	}

	return builder.String()
}

func (eps *EdgePropertySchema) IndexName() string {
	if eps.Type.Name == "STRING" {
		return fmt.Sprintf("%s(%d)", eps.Name, eps.Type.IndexLength)
	} else {
		return eps.Name
	}
}
