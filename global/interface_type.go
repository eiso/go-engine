package global

import "github.com/chrislusf/gleam/util"

//TODO: this needs a far more elegant solution
// how to have a reader that is global across all
// packages, since putting it in `git` creates a
// circular dependency
type Reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}
