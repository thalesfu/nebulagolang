package main

import (
	"fmt"
	"github.com/thalesfu/nebulagolang"
)

func main() {
	db, ok := nebulagolang.LoadDB()

	if !ok {
		return
	}

	execute, b, err := db.Execute("show spaces")

	if !b {
		fmt.Println(err)
	}

	nebulagolang.PrintTable(execute)
}
