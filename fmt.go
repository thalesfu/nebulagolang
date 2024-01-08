package nebulagolang

import (
	"fmt"
	nebulago "github.com/vesoft-inc/nebula-go/v3"
	"os"
	"strings"
	"text/tabwriter"
)

func PrintTable(resultSet *nebulago.ResultSet) {
	tb := resultSet.AsStringTable()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 5, ' ', 0)

	for _, row := range tb {
		fmt.Fprintln(w, strings.Join(row, "\t"))
	}

	w.Flush()
}
