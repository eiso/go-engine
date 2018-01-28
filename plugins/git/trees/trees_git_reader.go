package trees

import (
	"errors"
	"strconv"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreesGitReader struct {
	repositoryID string
	treeIter     *object.TreeIter
	fileIter     *object.FileIter
	lastTreeHash string
}

func New(r *git.Repository, path string) *TreesGitReader {
	treeIter, _ := r.TreeObjects()

	return &TreesGitReader{
		repositoryID: path,
		treeIter:     treeIter,
	}
}

func (r *TreesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"treeHash",
		"fileName",
		"blobHash",
		"blobSize",
		"isBinary",
	}

	return fieldNames, nil
}

/*
root
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- path: string (nullable = false)
 |-- blob: string (nullable = false)
*/

func (r *TreesGitReader) Read() (row *util.Row, err error) {
	if r.fileIter == nil {
		tree, err := r.treeIter.Next()
		if err != nil {
			return nil, errors.New("end of treeIter")
		}
		r.lastTreeHash = tree.Hash.String()
		r.fileIter = tree.Files()
	}

	file, err := r.fileIter.Next()
	if err != nil {
		r.fileIter = nil
		return nil, nil
	}

	binary, _ := file.IsBinary()

	return util.NewRow(util.Now(),
		r.repositoryID,
		file.Blob.Hash.String(),
		file.Name,
		r.lastTreeHash,
		file.Blob.Size,
		strconv.FormatBool(binary),
	), nil
}
