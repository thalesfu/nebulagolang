package nebulagolang

import (
	"strings"
)

type NoDataError struct {
	message string
}

func (e *NoDataError) Error() string {
	return e.message
}

func NoData(message string) *NoDataError {
	return &NoDataError{
		message: message,
	}
}

var NoDataErr = NoData("Not found data")

func CountByQuery(space *Space, query string) *ResultT[int64] {
	r := space.Execute(CommandPipelineCombine(query, "yield count(1) as count"))

	if !r.Ok {
		return NewResultT[int64](r)
	}

	if len(r.DataSet.GetRows()) == 0 {
		return NewErrorResultT[int64](NoData("Not found data by command: " + strings.Join(r.Commands, "")))
	}

	values, err := r.DataSet.GetValuesByColName("count")

	if err != nil {
		return NewErrorResultT[int64](err)
	}

	value, err := values[0].AsInt()

	if err != nil {
		return NewErrorResultT[int64](err)
	}

	return NewResultTWithData(r, value)
}
