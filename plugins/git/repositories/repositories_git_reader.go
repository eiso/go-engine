package repositories

import (
	"errors"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

type RepositoriesGitReader struct {
	repositoryID string
	repo         *git.Repository
}

func New(r *git.Repository, path string) *RepositoriesGitReader {

	return &RepositoriesGitReader{
		repo:         r,
		repositoryID: path,
	}
}

func (r *RepositoriesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"repositoryURLs",
		"headRef",
	}
	return fieldNames, nil
}

//TODO: add is_fork
func (r *RepositoriesGitReader) Read() (row *util.Row, err error) {

	repository := r.repo
	if err != nil {
		return nil, err
	}

	//TODO: check the remotes list results against results from native git
	// for repositories with many remotes, right now it only goes on [0]
	listRemotes, err := repository.Remotes()
	if err != nil {
		return nil, err
	}
	remoteURLs := listRemotes[0].Config().URLs

	head, err := repository.Head()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, head.Hash().String(), remoteURLs), errors.New("repository read")
}
