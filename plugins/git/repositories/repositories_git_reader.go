package repositories

import (
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

type RepositoriesGitReader struct {
	repo *git.Repository
}

func New(r *git.Repository) *RepositoriesGitReader {
	return &RepositoriesGitReader{
		repo: r,
	}
}

func (r *RepositoriesGitReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}

/*
root
 |-- id: string (nullable = false)
 |-- urls: array (nullable = false)
 |    |-- element: string (containsNull = false)
 |-- is_fork: boolean (nullable = true)
 |-- repository_path: string (nullable = true)
*/

func (r *RepositoriesGitReader) Read() (row *util.Row, err error) {

	// ... retrieving the branch being pointed by HEAD
	ref, err := r.repo.Head()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), ref.String()), nil
}
