package nebulagolang

import nebulago "github.com/vesoft-inc/nebula-go/v3"

type Result struct {
	Commands []string
	DataSet  *nebulago.ResultSet
	Ok       bool
	Err      error
}

func NewResult(dataset *nebulago.ResultSet, ok bool, err error, commands ...string) *Result {
	return &Result{
		Commands: commands,
		DataSet:  dataset,
		Ok:       ok,
		Err:      err,
	}
}

func NewErrorResult(err error) *Result {
	return &Result{
		Ok:  false,
		Err: err,
	}
}

func NewSuccessResult(commands ...string) *Result {
	return &Result{
		Commands: commands,
		Ok:       true,
	}
}

type ResultT[T any] struct {
	*Result
	Data T
}

func NewResultT[T any](result *Result) *ResultT[T] {
	return &ResultT[T]{
		Result: result,
	}
}

func NewResultTWithData[T any](result *Result, data T) *ResultT[T] {
	return &ResultT[T]{
		Result: result,
		Data:   data,
	}
}

func NewResultTWithError[T any](result *Result, err error) *ResultT[T] {
	result.Err = err
	return &ResultT[T]{
		Result: result,
	}
}

func NewErrorResultT[T any](err error) *ResultT[T] {
	return &ResultT[T]{
		Result: NewErrorResult(err),
	}
}

func NewSuccessResultT[T any](commands ...string) *ResultT[T] {
	return &ResultT[T]{
		Result: NewSuccessResult(commands...),
	}
}
