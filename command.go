package nebulagolang

import "strings"

func CommandPipelineCombine(stmts ...string) string {
	return strings.Join(stmts, " | ")
}
