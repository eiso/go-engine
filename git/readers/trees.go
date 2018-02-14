package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Trees struct {
	repositoryID string
	repo         *git.Repository
	commitsIter  object.CommitIter
	fileIter     *object.FileIter
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
	if r.fileIter == nil {
		c, err := r.commitsIter.Next()
		if err != nil {
			return nil, err
		}
		r.commitHash = c.Hash.String()
		r.fileIter, err = c.Files()
		if err != nil {
			return nil, err
		}
	}

	file, err := r.fileIter.Next()
	if err == io.EOF {
		r.fileIter = nil
		return r.Read()
	} else if err != nil {
		return nil, errors.Wrap(err, "could not get next file")
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		r.commitHash,
		file.Blob.Hash.String(),
		file.Name,
	), nil
}
