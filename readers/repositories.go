package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
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

	// // TODO: split remotes properly, for siva into seperate repos
	// listRemotes, err := repository.Remotes()
	// if err != nil {
	// 	return nil, errors.Wrap(err, "could not list remotes")
	// }

	// log.Printf("Log remotes: %v", listRemotes)
	// for k, _ := range listRemotes {
	// 	log.Printf("%v : %v : %v",
	// 		listRemotes[k].Config().Name,
	// 		listRemotes[k].Config().URLs,
	// 		listRemotes[k].Config().Fetch)
	// }

	var headHash string
	// Errors are not handles since some repositories can have an empty/unresolvable HEAD
	head, err := repository.Head()
	if err == nil {
		// if HEAD exists, returns the resolved hash
		headRef, err := resolveRef(repository, head)
		if err == nil {
			headHash = headRef.String()
		}
	}

	return util.NewRow(util.Now(), r.repositoryID, headHash), nil
}

func (r *Repositories) Close() error {
	return nil
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
