package blobs

import (
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Reader struct {
	repositoryID string
	blobs        *object.BlobIter
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	blobs, err := repo.BlobObjects()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch blob objects for repository")
	}

	return &Reader{
		repositoryID: path,
		blobs:        blobs,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"blobHash",
		"blobSize",
	}, nil
}

func (r *Reader) Read() (*util.Row, error) {
	blob, err := r.blobs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, blob.Hash.String(), blob.Size), nil
}
