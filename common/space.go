package common

import (
	"github.com/thalesfu/nebulagolang"
)

func GetSpace() (space *nebulagolang.Space) {

	db, ok := nebulagolang.LoadDB()

	if !ok {
		return
	}

	return db.Use("htldevelopandefficacygraphdb")
}
