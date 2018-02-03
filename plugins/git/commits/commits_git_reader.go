package commits

import (
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Reader struct {
	repositoryID string
	commits      object.CommitIter
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	commits, err := repo.CommitObjects()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch commit objects for repository")
	}

	return &Reader{
		repositoryID: path,
		commits:      commits,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"commitHash",
		"treeHash",
		"parentHashes",
		"parentsCount",
		"message",
		"authorEmail",
		"authorName",
		"authorDate",
		"committerEmail",
		"committerName",
		"committerDate",
	}, nil
}

func (r *Reader) Read() (*util.Row, error) {
	commit, err := r.commits.Next()
	if err != nil {
		// do not wrap this error, as it could be an io.EOF.
		return nil, err
	}

	var parentHashes []string
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		commit.Hash.String(),
		commit.TreeHash.String(),
		parentHashes,
		len(parentHashes),
		commit.Message,
		commit.Author.Email,
		commit.Author.Name,
		commit.Author.When.Unix(),
		commit.Committer.Email,
		commit.Committer.Name,
		commit.Committer.When.Unix(),
	), nil
}
