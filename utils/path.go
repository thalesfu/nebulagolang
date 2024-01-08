package utils

import "os"

func PathExists(file string) bool {
	ok, _ := PathExistsWithError(file)
	return ok
}

func PathExistsWithError(file string) (bool, error) {
	_, err := os.Stat(file)
	return err == nil || os.IsExist(err), err
}
