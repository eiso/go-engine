package trees

import (
	"errors"
	"strconv"
	"strings"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreesGitReader struct {
	repositoryID string
	trees        *object.TreeIter
	files        map[string]fileObject
}

type fileObject struct {
	treeHash string
	isBinary bool
	blobHash string
	blobSize int64
}

func New(r *git.Repository) *TreesGitReader {

	remotes, _ := r.Remotes()
	tree, _ := r.TreeObjects()

	urls := remotes[0].Config().URLs
	repositoryID := strings.TrimPrefix(urls[0], "https://")

	m := make(map[string]fileObject)

	// TODO missing folder entries, only files right now

	tree.ForEach(func(t *object.Tree) error {
		t.Files().ForEach(func(file *object.File) error {
			b, _ := file.IsBinary()

			m[file.Name] = fileObject{
				treeHash: t.Hash.String(),
				isBinary: b,
				blobHash: file.Blob.Hash.String(),
				blobSize: file.Blob.Size,
			}
			return nil
		})
		return nil
	})

	return &TreesGitReader{
		repositoryID: repositoryID,
		trees:        tree,
		files:        m,
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
	for k, v := range r.files {
		defer delete(r.files, k)

		return util.NewRow(util.Now(),
			r.repositoryID,
			v.treeHash,
			k,
			v.blobHash,
			v.blobSize,
			//TODO add ToBool to utils
			strconv.FormatBool(v.isBinary),
		), nil
	}
	return nil, errors.New("end of files list")
}
