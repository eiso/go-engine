package trees

import (
	"io"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	"github.com/src-d/go-git/plumbing/storer"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Reader struct {
	repositoryID       string
	repo               *git.Repository
	refsIter           storer.ReferenceIter
	fileIter           *object.FileIter
	refHash            string
	treeHashFromCommit string
	flag               bool
}

func NewReader(r *git.Repository, path string, flag bool) (*Reader, error) {
	refsIter, err := r.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references from repository")
	}

	return &Reader{
		repositoryID: path,
		repo:         r,
		refsIter:     refsIter,
		flag:         flag,
	}, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"blobHash",
		"fileName",
		"treeHash",
		"blobSize",
		"isBinary",
	}, nil
}

func (r *Reader) Read() (*util.Row, error) {
	if r.fileIter == nil {
		ref, err := r.refsIter.Next()
		if err != nil {
			// do not wrap this error, as it could be an io.EOF.
			return nil, err
		}
		r.refHash = ref.Hash().String()

		commit, err := r.repo.CommitObject(ref.Hash())
		if err != nil {
			return nil, errors.Wrap(err, "could not fetch commit object")
		}

		treeHash := commit.TreeHash
		tree, err := r.repo.TreeObject(treeHash)
		if err != nil {
			return nil, errors.Wrap(err, "could not fetch tree object")
		}
		r.treeHashFromCommit = treeHash.String()

		r.fileIter = tree.Files()
	}

	file, err := r.fileIter.Next()
	if err == io.EOF {
		r.fileIter = nil
		return nil, err
	} else if err != nil {
		return nil, errors.Wrap(err, "could not get next file")
	}

	binary, err := file.IsBinary()
	if err != nil {
		return nil, errors.Wrap(err, "could not check whether it's binary")
	}

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
