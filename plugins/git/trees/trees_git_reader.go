package trees

import (
	"fmt"

	"github.com/chrislusf/gleam/util"
	"github.com/src-d/go-git/plumbing/storer"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreesGitReader struct {
	repositoryID       string
	repo               *git.Repository
	refsIter           storer.ReferenceIter
	fileIter           *object.FileIter
	refHash            string
	treeHashFromCommit string
	flag               bool
}

func New(r *git.Repository, path string, flag bool) *TreesGitReader {
	refsIter, _ := r.References()

	return &TreesGitReader{
		repositoryID: path,
		repo:         r,
		refsIter:     refsIter,
		flag:         flag,
	}
}

func (r *TreesGitReader) ReadHeader() (fieldNames []string, err error) {
	fieldNames = []string{
		"repositoryID",
		"blobHash",
		"fileName",
		"treeHash",
		"blobSize",
		"isBinary",
	}

	return fieldNames, nil
}

func (r *TreesGitReader) Read() (row *util.Row, err error) {
	if r.fileIter == nil {

		ref, err := r.refsIter.Next()
		if err != nil {
			return nil, fmt.Errorf("end of refsIter")
		}
		r.refHash = ref.Hash().String()

		commit, err := r.repo.CommitObject(ref.Hash())
		if err != nil {
			return nil, err
		}

		treeHash := commit.TreeHash
		tree, err := r.repo.TreeObject(treeHash)
		if err != nil {
			return nil, err
		}
		r.treeHashFromCommit = treeHash.String()

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
		r.refHash,
		r.treeHashFromCommit,
		file.Blob.Hash.String(),
		file.Name,
		file.Blob.Size,
		binary,
	), nil
}
