package references

import (
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type Reader struct {
	repositoryID string
	refs         storer.ReferenceIter
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references from repository")
	}
	return &Reader{
		repositoryID: path,
		refs:         refs,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"refHash",
		"refName",
		"commitHash",
	}, nil
}

//TODO: add is_remote
func (r *Reader) Read() (*util.Row, error) {
	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, ref.Hash().String(), ref.Name().String()), nil
}
