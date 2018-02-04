package commits

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
	commits      object.CommitIter

	repositories *util.Row
	references   *util.Row

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

	if _, ok := readers["references"]; !ok {
		commits, err := repo.CommitObjects()
		if err != nil {
			return nil, errors.Wrap(err, "could not fetch commit objects for repository")
		}

		reader.commits = commits
		return reader, nil
	}

	return reader, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"commitHash",
		"treeHash",
		"parentHashes",
		"parentsCount",
		"message",
		"authorEmail",
		"authorName",
		"authorDate",
		"committerEmail",
		"committerName",
		"committerDate",
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
		if r.commits == nil {
			ref, err := refReader.Read()
			if err != nil {
				return nil, err
			}
			if ref != nil {
				r.references = ref
			}
			// refHash is the 2rd column in references_git_reader
			refHash := ref.V[0].(plumbing.Hash)
			r.commits, err = r.repo.Log(&git.LogOptions{From: refHash})
			if err != nil {
				return nil, err
			}
		}
	}

	commit, err := r.commits.Next()
	//TODO: messy, but don't know how else to deal with it
	if _, ok := r.readers["references"]; ok {
		if err == io.EOF {
			r.commits = nil
			return r.Read()
		}
	}
	if err != nil {
		return nil, err
	}

	var parentHashes []string
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
	}

	row := util.NewRow(util.Now(),
		r.repositoryID,
		commit.Hash,
		commit.TreeHash.String(),
		parentHashes,
		len(parentHashes),
		commit.Message,
		commit.Author.Email,
		commit.Author.Name,
		commit.Author.When.Unix(),
		commit.Committer.Email,
		commit.Committer.Name,
		commit.Committer.When.Unix(),
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

	return row, nil
}
