package blobs

import (
	"strings"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type BlobsGitReader struct {
	repositoryID string
	blobs        *object.BlobIter
}

func New(r *git.Repository) *BlobsGitReader {

	remotes, _ := r.Remotes()
	blobs, _ := r.BlobObjects()

	urls := remotes[0].Config().URLs
	repositoryID := strings.TrimPrefix(urls[0], "https://")

	return &BlobsGitReader{
		repositoryID: repositoryID,
		blobs:        blobs,
	}
}

func (r *BlobsGitReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}

/*
root
 |-- blob_id: string (nullable = false)
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- content: binary (nullable = true)
 |-- is_binary: boolean (nullable = false)
 |-- path: string (nullable = false)
*/

func (r *BlobsGitReader) Read() (row *util.Row, err error) {

	blob, err := r.blobs.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(), r.repositoryID, blob.Hash.String()), nil
}
