package repositories

import (
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type RepositoriesGitReader struct {
	repo         *git.Repository
	path         string
	repositoryID string
	urls         []string
	refs         storer.ReferenceIter
}

func New(r *git.Repository, path string) *RepositoriesGitReader {

	refs, _ := r.References()
	remotes, _ := r.Remotes()
	urls := remotes[0].Config().URLs

	return &RepositoriesGitReader{
		repo:         r,
		repositoryID: path,
		urls:         urls,
		refs:         refs,
	}
}

func (r *RepositoriesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"repositoryPath",
		"repositoryURLs",
	}
	return fieldNames, nil
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

	return util.NewRow(util.Now(), r.repositoryID, r.urls), nil
}
