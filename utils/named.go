package utils

import (
	"log"

	"github.com/chrislusf/gleam/gio"
)

// GetByNameFunc returns value by name for a row
type GetByNameFunc func([]interface{}, string) interface{}
type namedMapperFunc func(row []interface{}, getByName GetByNameFunc) error

// NamedMapper returns gio.Mapper with support for selecting columns by name
func NamedMapper(names []string, fn namedMapperFunc) gio.Mapper {
	headers := make(map[string]int)

	getByName := func(row []interface{}, name string) interface{} {
		val, ok := headers[name]
		if !ok {
			log.Panicf("column '%s' not found", name)
		}
		return row[val]
	}

	return func(row []interface{}) error {
		if len(headers) == 0 {
			for i, h := range row {
				headers[h.(string)] = i
			}
			if len(names) > 0 {
				return gio.Emit(stringsToInterfaces(names)...)
			}
			return nil
		}

		return fn(row, getByName)
	}
}

func stringsToInterfaces(st []string) []interface{} {
	interfaces := make([]interface{}, len(st))
	for i, s := range st {
		interfaces[i] = s
	}
	return interfaces
}
