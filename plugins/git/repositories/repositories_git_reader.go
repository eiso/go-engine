package repositories

import (
	"strings"

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

func New(path string, r *git.Repository) *RepositoriesGitReader {

	refs, _ := r.References()
	remotes, _ := r.Remotes()

	urls := remotes[0].Config().URLs
	repositoryID := strings.TrimPrefix(urls[0], "https://")

	return &RepositoriesGitReader{
		repo:         r,
		path:         path,
		repositoryID: repositoryID,
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

	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	key := util.Hash([]byte(ref.Hash().String() + r.repositoryID))

	return util.NewRow(util.Now(), key, r.repositoryID, r.path, r.urls), nil
}
