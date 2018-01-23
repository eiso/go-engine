package blobs

import (
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

type BlobsGitReader struct {
	//blobs someblobTypefromGoGit
}

func New(r *git.Repository) *BlobsGitReader {

	//blobs, _ := ...

	return &BlobsGitReader{
	//blobs: blobs,
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

	return nil, err
}
