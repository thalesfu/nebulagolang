package nebulagolang

import (
	"fmt"
	"github.com/thalesfu/golangutils"
	"reflect"
)

type EID struct {
	from     string
	to       string
	edgeName string
	hasRank  bool
	rank     int
}

func (e *EID) String() string {
	if e.hasRank {
		return fmt.Sprintf("\"%s\"->\"%s\"@%d", e.from, e.to, e.rank)
	}

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

func (e *EID) Rank() int {
	return e.rank
}

func (e *EID) HasRank() bool {
	return e.hasRank
}

func (e *EID) SetRank(rank int) {
	e.hasRank = true
	e.rank = rank
}

func NewEID(from string, to string, t string) *EID {
	return &EID{from: from, to: to, edgeName: t}
}

func NewEIDWithRank(from string, to string, rank int, t string) *EID {
	eid := NewEID(from, to, t)
	eid.SetRank(rank)
	return eid
}

func GetEIDByEdge(e interface{}) *EID {
	return GetEIDByEdgeReflectValue(reflect.ValueOf(e))
}

func GetEIDByEdgeReflectValue(v reflect.Value) *EID {
	valueOfVertex := golangutils.IndirectValue(v)
	typeOfVertex := valueOfVertex.Type()

	hasRank := hasEdgeRank(typeOfVertex)

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

		if hasRank && ft.Tag.Get("nebulakey") == "edgerank" {
			eid.SetRank(fv.Interface().(int))
		}

		if hasRank {
			if eid.edgeName != "" && eid.from != "" && eid.to != "" && eid.hasRank {
				return eid
			}
		} else {
			if eid.edgeName != "" && eid.from != "" && eid.to != "" {
				return eid
			}
		}
	}

	return eid
}
