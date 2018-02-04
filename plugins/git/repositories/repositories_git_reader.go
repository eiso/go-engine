package repositories

import (
	"io"

	"github.com/chrislusf/gleam/plugins/git/global"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
)

type Reader struct {
	repositoryID string
	repos        *reposIter
	pos          int

	readers map[string]global.Reader
	options *Options
}

type Options struct {
	filter  map[int][]string
	reverse bool
}

func NewOptions(a map[int][]string, b bool) (*Options, error) {
	return &Options{
		filter:  a,
		reverse: b,
	}, nil
}

func NewReader(repo *git.Repository, path string, options *Options, readers map[string]global.Reader) (*Reader, error) {
	return &Reader{
		repos:        &reposIter{repos: []*git.Repository{repo}},
		repositoryID: path,
		readers:      readers,
		options:      options,
	}, nil
}

func (r *Reader) ReadHeader() (fieldNames []string, err error) {
	return []string{
		"repositoryID",
		"repositoryURLs",
		"headRef",
	}, nil
}

//TODO: add is_fork
func (r *Reader) Read() (*util.Row, error) {
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

	return util.NewRow(util.Now(), r.repositoryID, "JACK", head.Hash().String(), remoteURLs), nil
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
