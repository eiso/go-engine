package source

import "github.com/chrislusf/gleam/util"

type Reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}
