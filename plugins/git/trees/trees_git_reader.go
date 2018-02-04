package trees

import (
	"io"

	"github.com/chrislusf/gleam/plugins/git/global"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type Reader struct {
	repositoryID string
	repo         *git.Repository
	fileIter     *object.FileIter

	repositories *util.Row
	references   *util.Row
	commits      *util.Row

	readers map[string]global.Reader
	options *Options
}

type Options struct {
	filter  map[int][]string
	reverse bool
}

func NewOptions(a map[int][]string, b bool) (*Options, error) {
	return &Options{
		filter:  a,
		reverse: b,
	}, nil
}

func NewReader(repo *git.Repository, path string, options *Options, readers map[string]global.Reader) (*Reader, error) {
	reader := &Reader{repositoryID: path,
		repo:    repo,
		options: options,
		readers: readers,
	}
	return reader, nil
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

	if repoReader, ok := r.readers["repositories"]; ok {
		row, err := repoReader.Read()
		if err != io.EOF && err != nil {
			return nil, err
		}
		if row != nil {
			r.repositories = row
		}
	}

	if refReader, ok := r.readers["references"]; ok {
		row, err := refReader.Read()
		if err != io.EOF && err != nil {
			return nil, err
		}
		if row != nil {
			r.references = row
		}
	}

	if commitsReader, ok := r.readers["commits"]; ok {
		if r.fileIter == nil {
			commit, err := commitsReader.Read()
			if err != nil {
				// do not wrap this error, as it could be an io.EOF.
				return nil, err
			}
			r.commits = commit
			treeHash := commit.V[1].(plumbing.Hash)
			tree, err := r.repo.TreeObject(treeHash)
			if err != nil {
				return nil, errors.Wrap(err, "could not fetch tree object")
			}
			r.fileIter = tree.Files()
		}
	}

	file, err := r.fileIter.Next()
	if err == io.EOF {
		r.fileIter = nil
		return r.Read()
	}

	if err != nil {
		return nil, errors.Wrap(err, "could not get next file")
	}

	binary, err := file.IsBinary()
	if err != nil {
		return nil, errors.Wrap(err, "could not check whether it's binary")
	}

	row := util.NewRow(util.Now(),
		r.repositoryID,
		file.Blob.Hash.String(),
		file.Name,
		file.Blob.Size,
		binary,
	)

	if _, ok := r.readers["references"]; ok {
		for _, v := range r.references.V {
			row = row.AppendValue(v)
		}
	}

	if _, ok := r.readers["repositories"]; ok {
		for _, v := range r.repositories.V {
			row = row.AppendValue(v)
		}
	}

	if _, ok := r.readers["commits"]; ok {
		for _, v := range r.commits.V {
			row = row.AppendValue(v)
		}
	}

	return row, nil
}
