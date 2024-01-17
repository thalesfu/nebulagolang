package nebulagolang

import nebulago "github.com/vesoft-inc/nebula-go/v3"

type Result struct {
	Commands []string
	DataSet  *nebulago.ResultSet
	Ok       bool
	Err      error
}

func newResult(dataset *nebulago.ResultSet, ok bool, err error, commands ...string) *Result {
	return &Result{
		Commands: commands,
		DataSet:  dataset,
		Ok:       ok,
		Err:      err,
	}
}

func newErrorResult(err error) *Result {
	return &Result{
		Ok:  false,
		Err: err,
	}
}

func newSuccessResult(commands ...string) *Result {
	return &Result{
		Commands: commands,
		Ok:       true,
	}
}

type ResultT[T any] struct {
	*Result
	Data T
}

func newResultT[T any](result *Result) *ResultT[T] {
	return &ResultT[T]{
		Result: result,
	}
}

func newResultTWithData[T any](result *Result, data T) *ResultT[T] {
	return &ResultT[T]{
		Result: result,
		Data:   data,
	}
}

func newResultTWithError[T any](result *Result, err error) *ResultT[T] {
	result.Err = err
	return &ResultT[T]{
		Result: result,
	}
}

func newErrorResultT[T any](err error) *ResultT[T] {
	return &ResultT[T]{
		Result: newErrorResult(err),
	}
}

func newSuccessResultT[T any](commands ...string) *ResultT[T] {
	return &ResultT[T]{
		Result: newSuccessResult(commands...),
	}
}
