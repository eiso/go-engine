package references

import (
	"strings"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type ReferencesGitReader struct {
	repository_id string
	refs          storer.ReferenceIter
}

func New(r *git.Repository) *ReferencesGitReader {

	refs, _ := r.References()
	remotes, _ := r.Remotes()

	urls := remotes[0].Config().URLs
	repository_id := strings.TrimPrefix(urls[0], "https://")

	return &ReferencesGitReader{
		repository_id: repository_id,
		refs:          refs,
	}
}

func (r *ReferencesGitReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}

/*
root
 |-- repository_id: string (nullable = false)
 |-- name: string (nullable = false)
 |-- hash: string (nullable = false)
 |-- is_remote: boolean (nullable = false)
*/

func (r *ReferencesGitReader) Read() (row *util.Row, err error) {

	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repository_id, ref.Hash().String(), ref.Name().String()), nil
}
