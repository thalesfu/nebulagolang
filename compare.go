package nebulagolang

import (
	"errors"
	"github.com/thalesfu/nebulagolang/utils"
	"reflect"
)

type CompareResult[T interface{}] struct {
	Added   []T
	Deleted []T
	Updated []T
	Kept    []T
}

const batchExecuteCount = 250

func NewCompareResult[T interface{}]() *CompareResult[T] {
	return &CompareResult[T]{
		Added:   make([]T, 0),
		Deleted: make([]T, 0),
		Updated: make([]T, 0),
		Kept:    make([]T, 0),
	}
}

func IsSameNebulaEntity[T interface{}](a T, b T) bool {
	if !IsSameNebulaEntityID(a, b) {
		return false
	}

	return IsSameNebulaProperty(a, b)
}

func IsSameNebulaEntityID[T interface{}](a T, b T) bool {
	ok, _ := IsVertex[T]()

	if ok {
		return GetVID(a) == GetVID(b)
	}

	ok, _ = IsEdge[T]()

	if ok {
		return GetEIDByEdge(a).String() == GetEIDByEdge(b).String()
	}

	return false
}

func IsSameNebulaProperty[T interface{}](a T, b T) bool {
	va := utils.IndirectValue(reflect.ValueOf(a))
	ta := va.Type()
	vb := utils.IndirectValue(reflect.ValueOf(b))

	for i := 0; i < ta.NumField(); i++ {
		fta := ta.Field(i)

		if fta.Tag.Get("nebulaproperty") != "" {
			if va.Field(i).Interface() != vb.Field(i).Interface() {
				return false
			}
		}
	}

	return true
}

func CompareNebulaEntitySlice[T interface{}](as []T, bs []T) *CompareResult[T] {
	am := make(map[string]T)
	bm := make(map[string]T)

	ok, _ := IsVertex[T]()

	if ok {
		for _, a := range as {
			am[GetVID(a)] = a
		}

		for _, b := range bs {
			bm[GetVID(b)] = b
		}
	} else {
		ok, _ := IsEdge[T]()
		if ok {
			for _, a := range as {
				am[GetEIDByEdge(a).String()] = a
			}

			for _, b := range bs {
				bm[GetEIDByEdge(b).String()] = b
			}
		}
	}

	return CompareNebulaEntityMap(am, bm)
}

func CompareNebulaEntityMap[T interface{}](am map[string]T, bm map[string]T) *CompareResult[T] {
	result := NewCompareResult[T]()
	baseMap := make(map[string]T)
	for k, a := range am {
		baseMap[k] = a
	}

	for k, b := range bm {
		if a, ok := baseMap[k]; ok {
			if IsSameNebulaEntity(a, b) {
				result.Kept = append(result.Kept, b)
			} else {
				result.Updated = append(result.Updated, b)
			}
			delete(baseMap, k)
		} else {
			result.Added = append(result.Added, b)
		}
	}

	for _, a := range baseMap {
		result.Deleted = append(result.Deleted, a)
	}

	return result
}

func CompareAndUpdateNebulaEntityBySliceAndQuery[T interface{}](space *Space, ns []T, query string) (*Result, *CompareResult[T]) {
	ok, _ := IsVertex[T]()

	if ok {
		return CompareAndUpdateVertexesBySliceAndQuery[T](space, ns, query)
	}

	ok, _ = IsEdge[T]()

	if ok {
		return CompareAndUpdateEdgesBySliceAndQuery[T](space, ns, query)
	}

	return NewErrorResult(errors.New("not a vertex or edge")), nil
}

func CompareAndUpdateNebulaEntityByMapAndQuery[T interface{}](space *Space, nm map[string]T, query string) (*Result, *CompareResult[T]) {
	ok, _ := IsVertex[T]()

	if ok {
		return CompareAndUpdateVertexesByMapAndQuery[T](space, nm, query)
	}

	ok, _ = IsEdge[T]()

	if ok {
		return CompareAndUpdateEdgesByMapAndQuery[T](space, nm, query)
	}

	return NewErrorResult(errors.New("not a vertex or edge")), nil
}

func CompareAndUpdateVertexesBySliceAndQuery[T interface{}](space *Space, ns []T, query string) (*Result, *CompareResult[T]) {
	nm := make(map[string]T)
	for _, n := range ns {
		nm[GetVID(n)] = n
	}

	return CompareAndUpdateVertexesByMapAndQuery[T](space, nm, query)
}

func CompareAndUpdateVertexesByMapAndQuery[T interface{}](space *Space, nm map[string]T, query string) (*Result, *CompareResult[T]) {
	cmds := make([]string, 0)
	result := GetAllVertexesByQuery[T](space, query)

	if !result.Ok {
		return result.Result, nil
	}

	cmds = append(cmds, result.Commands...)

	compareResult := CompareNebulaEntityMap[T](result.Data, nm)

	if len(compareResult.Added) > 0 {
		insertResult := BatchInsertVertexes(space, batchExecuteCount, compareResult.Added)
		if !insertResult.Ok {
			return insertResult, nil
		}

		cmds = append(cmds, insertResult.Commands...)
	}

	if len(compareResult.Updated) > 0 {
		updateResult := BatchUpdateVertexes(space, batchExecuteCount, compareResult.Updated)
		if !updateResult.Ok {
			return updateResult, nil
		}

		cmds = append(cmds, updateResult.Commands...)
	}

	if len(compareResult.Deleted) > 0 {
		deleteResult := BatchDeleteVertexes(space, batchExecuteCount, compareResult.Deleted)
		if !deleteResult.Ok {
			return deleteResult, nil
		}

		cmds = append(cmds, deleteResult.Commands...)
	}

	return NewSuccessResult(cmds...), compareResult
}

func CompareAndUpdateEdgesBySliceAndQuery[T interface{}](space *Space, ns []T, query string) (*Result, *CompareResult[T]) {
	nm := make(map[string]T)
	for _, n := range ns {
		nm[GetEIDByEdge(n).String()] = n
	}

	return CompareAndUpdateEdgesByMapAndQuery(space, nm, query)
}

func CompareAndUpdateEdgesByMapAndQuery[T interface{}](space *Space, nm map[string]T, query string) (*Result, *CompareResult[T]) {
	cmds := make([]string, 0)
	result := GetAllEdgesByQuery[T](space, query)

	if !result.Ok {
		return result.Result, nil
	}

	cmds = append(cmds, result.Commands...)

	compareResult := CompareNebulaEntityMap[T](result.Data, nm)

	if len(compareResult.Added) > 0 {
		insertResult := BatchInsertEdges(space, batchExecuteCount, compareResult.Added)
		if !insertResult.Ok {
			return insertResult, nil
		}

		cmds = append(cmds, insertResult.Commands...)
	}

	if len(compareResult.Updated) > 0 {
		updateResult := BatchUpdateEdges(space, batchExecuteCount, compareResult.Updated)
		if !updateResult.Ok {
			return updateResult, nil
		}

		cmds = append(cmds, updateResult.Commands...)
	}

	if len(compareResult.Deleted) > 0 {
		deleteResult := BatchDeleteEdges(space, batchExecuteCount, compareResult.Deleted)
		if !deleteResult.Ok {
			return deleteResult, nil
		}

		cmds = append(cmds, deleteResult.Commands...)
	}

	return NewSuccessResult(cmds...), compareResult
}
