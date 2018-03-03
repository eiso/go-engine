package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Trees struct {
	repositoryID string
	repo         *git.Repository
	commitsIter  object.CommitIter
	treeIter     *object.TreeWalker
	commitHash   string
}

func NewTrees(r *git.Repository, path string, commitsIter object.CommitIter) (*Trees, error) {
	return &Trees{
		repositoryID: path,
		repo:         r,
		commitsIter:  commitsIter,
	}, nil
}

func (r *Trees) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"commitHash",
		"blobHash",
		"fileName",
	}, nil
}

func (r *Trees) Read() (*util.Row, error) {
	if r.treeIter == nil {
		c, err := r.commitsIter.Next()
		if err != nil {
			return nil, err
		}
		r.commitHash = c.Hash.String()
		tree, err := c.Tree()
		if err != nil {
			return nil, err
		}
		seen := make(map[plumbing.Hash]bool)
		r.treeIter = object.NewTreeWalker(tree, true, seen)
	}

	name, entry, err := r.treeIter.Next()
	if err == io.EOF {
		r.treeIter = nil
		return r.Read()
	} else if err != nil {
		return nil, errors.Wrap(err, "could not get next file")
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		r.commitHash,
		entry.Hash.String(),
		name,
	), nil
}

func (r *Trees) Close() error {
	if r.commitsIter != nil {
		r.commitsIter.Close()
	}
	if r.treeIter != nil {
		r.treeIter.Close()
	}
	return nil
}
