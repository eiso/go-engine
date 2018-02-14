package source

import "github.com/chrislusf/gleam/util"

type SourceReaders interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}
