package udf

import (
	"github.com/chrislusf/gleam/gio"
	enry "gopkg.in/src-d/enry.v1"
)

// ClassifyLanguage TODO
func ClassifyLanguage(filenameIdx, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[filenameIdx])
		content := gio.ToBytes(x[contentIdx])
		lang := enry.GetLanguage(filename, content)
		return gio.Emit(append(x, lang)...)
	}
}
