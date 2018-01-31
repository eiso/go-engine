package references

import (
	"io"
	"strconv"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type Reader struct {
	repositoryID string
	repo         *git.Repository
	refs         storer.ReferenceIter
	commitsIter  object.CommitIter

	refCommitHash string
	refName       string
	refIsRemote   string
}

func NewReader(repo *git.Repository, path string) (*Reader, error) {
	refs, err := repo.References()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch references from repository")
	}
	return &Reader{
		repositoryID: path,
		repo:         repo,
		refs:         refs,
	}, nil
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
	if r.commitsIter == nil {
		ref, err := r.refs.Next()
		if err != nil {
			return nil, err
		}

		// Get correct commit hash
		// there is Repository.ResolveRevision but it fails on some tags and performance is worst
		refCommitHash := ref.Hash()
		// handle symbolic references like HEAD
		if ref.Type() == plumbing.SymbolicReference {
			targetRef, _ := r.repo.Reference(ref.Target(), true)
			refCommitHash = targetRef.Hash()
		}

		// handle tag references
		tag, err := r.repo.TagObject(refCommitHash)
		if err == nil {
			commit, _ := tag.Commit()
			refCommitHash = commit.Hash
		}

		r.commitsIter, err = r.repo.Log(&git.LogOptions{From: refCommitHash})
		if err != nil {
			return nil, err
		}

		r.refCommitHash = refCommitHash.String()
		r.refName = ref.Name().String()
		r.refIsRemote = strconv.FormatBool(ref.Name().IsRemote())
	}

	commit, err := r.commitsIter.Next()
	if err == io.EOF {
		r.commitsIter = nil
		return r.Read()
	}
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		r.refCommitHash,
		r.refName,
		commit.Hash.String(),
		r.refIsRemote,
	), nil
}
