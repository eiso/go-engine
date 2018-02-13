package utils

import "github.com/chrislusf/gleam/gio"

// ColumnToKey moves the column i to be the index
func ColumnToKey(i int) gio.Mapper {
	return func(x []interface{}) error {
		row := append([]interface{}{x[i]}, x[:i]...)
		row = append(row, x[i+1:]...)
		return gio.Emit(row...)
	}
}
