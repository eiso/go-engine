package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Blobs struct {
	repositoryID string
	repo         *git.Repository
	commitsIter  object.CommitIter
	fileIter     *object.FileIter
	commitHash   string
}

func NewBlobs(r *git.Repository, path string, commitsIter object.CommitIter) (*Blobs, error) {
	return &Blobs{
		repositoryID: path,
		repo:         r,
		commitsIter:  commitsIter,
	}, nil
}

func (r *Blobs) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"blobHash",
		"commitHash",
		"content",
		"path",
		"isBinary",
		"blobSize",
	}, nil
}

func (r *Blobs) Read() (*util.Row, error) {
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

	content, err := file.Contents()
	if err != nil {
		return nil, errors.Wrap(err, "could not get file content")
	}

	binary, err := file.IsBinary()
	if err != nil {
		return nil, errors.Wrap(err, "could not check whether file is binary")
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		file.Blob.Hash.String(),
		r.commitHash,
		content,
		file.Name,
		binary,
		file.Blob.Size,
	), nil
}
