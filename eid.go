package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/nebulagolang/utils"
	"reflect"
)

type EID struct {
	from     string
	to       string
	edgeName string
}

func (e *EID) String() string {
	return fmt.Sprintf("\"%s\"->\"%s\"", e.from, e.to)
}

func (e *EID) From() string {
	return e.from
}

func (e *EID) To() string {
	return e.to
}

func (e *EID) Type() string {
	return e.edgeName
}

func NewEID(from string, to string, t string) *EID {
	return &EID{from: from, to: to, edgeName: t}
}

func GetEIDByEdge(e interface{}) *EID {
	return GetEIDByEdgeReflectValue(reflect.ValueOf(e))
}

func GetEIDByEdgeReflectValue(v reflect.Value) *EID {
	valueOfVertex := utils.IndirectValue(v)
	typeOfVertex := valueOfVertex.Type()

	eid := &EID{}

	for i := 0; i < typeOfVertex.NumField(); i++ {
		fv := valueOfVertex.Field(i)
		ft := typeOfVertex.Field(i)

		edgeName := ft.Tag.Get("nebulaedgename")
		if edgeName != "" {
			eid.edgeName = edgeName
		}

		if ft.Tag.Get("nebulakey") == "edgefrom" {
			eid.from = getVIDByVertexReflectValue(fv)
		}

		if ft.Tag.Get("nebulakey") == "edgeto" {
			eid.to = getVIDByVertexReflectValue(fv)
		}

		if eid.edgeName != "" && eid.from != "" && eid.to != "" {
			return eid
		}
	}

	return eid
}
