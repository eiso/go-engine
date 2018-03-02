package readers

import (
	"io"
	"strconv"

	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

var ErrRef = errors.New("unable to resolve reference")

type References struct {
	repositoryID string
	repo         *git.Repository
	refs         storer.ReferenceIter
	onlyRefs     []string
}

func NewReferences(repo *git.Repository, path string, onlyRefs []string) (*References, error) {
	return &References{
		repositoryID: path,
		repo:         repo,
		onlyRefs:     onlyRefs,
	}, nil
}

func (r *References) ReadHeader() ([]string, error) {
	return []string{
		"repositoryID",
		"refHash",
		"refName",
		"isRemote",
	}, nil
}

func (r *References) Read() (*util.Row, error) {
	if r.refs == nil {
		var err error
		r.refs, err = r.GetIter()
		if err != nil {
			return nil, err
		}
	}

	ref, err := r.refs.Next()
	if err != nil {
		return nil, err
	}

	refCommitHash, err := resolveRef(r.repo, ref)
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		refCommitHash.String(),
		ref.Name().String(),
		strconv.FormatBool(ref.Name().IsRemote()),
	), nil
}

func (r *References) GetIter() (storer.ReferenceIter, error) {
	var refs storer.ReferenceIter
	var err error

	if len(r.onlyRefs) > 0 {
		var refsNames []plumbing.ReferenceName
		for _, name := range r.onlyRefs {
			refsNames = append(refsNames, plumbing.ReferenceName(name))
		}
		refs = &refIterator{repo: r.repo, refNames: refsNames}
	} else {
		refs, err = r.repo.References()
		if err != nil {
			return nil, errors.Wrap(err, "could not fetch references from repository")
		}
	}

	return refs, err
}

type refIterator struct {
	repo     *git.Repository
	refNames []plumbing.ReferenceName
	pos      int
}

func (iter *refIterator) Next() (*plumbing.Reference, error) {
	if iter.pos >= len(iter.refNames) {
		return nil, io.EOF
	}
	refName := iter.refNames[iter.pos]
	ref, err := iter.repo.Reference(refName, true)
	if err != nil {
		return nil, err
	}
	iter.pos++
	return ref, nil
}

// ForEach call the cb function for each reference contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stopped but no error is returned. The iterator is closed.
func (iter *refIterator) ForEach(cb func(*plumbing.Reference) error) error {
	defer iter.Close()
	for {
		r, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := cb(r); err != nil {
			if err == storer.ErrStop {
				break
			}

			return err
		}
	}

	return nil
}

func (iter *refIterator) Close() {}

// Get correct commit hash
// there is Repository.ResolveRevision but it fails on some tags and performance is worst
func resolveRef(repo *git.Repository, ref *plumbing.Reference) (plumbing.Hash, error) {
	refCommitHash := ref.Hash()

	// handle symbolic references like HEAD
	if ref.Type() == plumbing.SymbolicReference {
		targetRef, err := repo.Reference(ref.Target(), true)
		if err != nil {
			return plumbing.NewHash(""), ErrRef
		}
		refCommitHash = targetRef.Hash()
	}

	// avoids handling tags
	_, err := repo.TagObject(refCommitHash)
	if err != nil {
		return plumbing.NewHash(""), ErrRef
	}

	if ref.Type() == plumbing.InvalidReference {
		return plumbing.NewHash(""), ErrRef
	}

	return refCommitHash, nil
}
