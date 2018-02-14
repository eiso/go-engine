package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
)

type Repositories struct {
	repositoryID string
	repos        *reposIter
}

func NewRepositories(repo *git.Repository, path string) (*Repositories, error) {
	return &Repositories{
		repos:        &reposIter{repos: []*git.Repository{repo}},
		repositoryID: path,
	}, nil
}

func (r *Repositories) ReadHeader() (fieldNames []string, err error) {
	return []string{
		"repositoryID",
		"repositoryURLs",
		"headRef",
	}, nil
}

//TODO: add is_fork
func (r *Repositories) Read() (*util.Row, error) {
	repository, err := r.repos.Next()
	if err != nil {
		// do not wrap this error, as it could be an io.EOF.
		return nil, err
	}

	//TODO: check the remotes list results against results from native git
	// for repositories with many remotes, right now it only goes on [0]
	listRemotes, err := repository.Remotes()
	if err != nil {
		return nil, errors.Wrap(err, "could not list remotes")
	}

	var remoteURLs []string
	if len(listRemotes) > 0 {
		remoteURLs = listRemotes[0].Config().URLs
	}

	head, err := repository.Head()
	if err != nil {
		return nil, errors.Wrap(err, "could not get head from repository")
	}

	return util.NewRow(util.Now(), r.repositoryID, head.Hash().String(), remoteURLs), nil
}

type reposIter struct {
	repos []*git.Repository
	pos   int
}

func (iter *reposIter) Next() (*git.Repository, error) {
	if iter.pos >= len(iter.repos) {
		return nil, io.EOF
	}
	repo := iter.repos[iter.pos]
	iter.pos++
	return repo, nil
}
