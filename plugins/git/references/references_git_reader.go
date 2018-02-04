package references

import (
	"io"

	"github.com/chrislusf/gleam/plugins/git/global"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type Reader struct {
	repositoryID string
	repo         *git.Repository
	refs         storer.ReferenceIter
	filtered     refsIter

	repositories *util.Row

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
	reader := &Reader{
		repositoryID: path,
		repo:         repo,
		options:      options,
		readers:      readers,
	}

	// TODO: figure out how to return storer.ReferenceIter
	// with the filteredRefNames instead of refsIter
	if options.filter[2] != nil {
		reader.filtered = filterRefNames(repo, options.filter[2])
		return reader, nil
	}

	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references from repository")
	}
	reader.refs = refs
	return reader, nil
}

func filterRefNames(r *git.Repository, refNames []string) refsIter {
	var refs refsIter
	for _, name := range refNames {
		ref, err := r.Storer.Reference(plumbing.ReferenceName(name))
		if err != nil {
			// continue when reference can't be found
			continue
		}
		refs.refs = append(refs.refs, ref)
	}
	return refs
}

type refsIter struct {
	refs []*plumbing.Reference
	pos  int
}

func (iter *refsIter) Next() (*plumbing.Reference, error) {
	if iter.pos >= len(iter.refs) {
		return nil, io.EOF
	}
	ref := iter.refs[iter.pos]
	iter.pos++
	return ref, nil
}

func (r *Reader) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"refHash",
		"refName",
		"commitHash",
		"isRemote",
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

	var ref *plumbing.Reference
	var err error

	if r.filtered.refs != nil {
		ref, err = r.filtered.Next()
		if err != nil {
			return nil, err
		}
	} else {
		ref, err = r.refs.Next()
		if err != nil {
			return nil, err
		}
	}

	// Get correct commit hash
	// there is Repository.ResolveRevision but it fails on some tags and performance is worst
	refHash := ref.Hash()
	// handle symbolic references like HEAD
	if ref.Type() == plumbing.SymbolicReference {
		targetRef, _ := r.repo.Reference(ref.Target(), true)
		refHash = targetRef.Hash()
	}

	// handle tag references
	tag, err := r.repo.TagObject(refHash)
	if err == nil {
		commit, _ := tag.Commit()
		refHash = commit.Hash
	}

	row := util.NewRow(util.Now(),
		r.repositoryID,
		refHash,
		ref.Name().String(),
		ref.Name().IsRemote(),
	)

	if _, ok := r.readers["repositories"]; ok {
		for _, v := range r.repositories.V {
			row = row.AppendValue(v)
		}
	}

	return row, nil
}
