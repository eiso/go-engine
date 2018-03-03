package readers

import (
	"io"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
	storer "gopkg.in/src-d/go-git.v4/plumbing/storer"
)

type Commits struct {
	repositoryID string
	repo         *git.Repository
	commitsIter  object.CommitIter
	refsIter     storer.ReferenceIter
	all          bool
}

func NewCommits(repo *git.Repository, path string, refsIter storer.ReferenceIter, all bool) (*Commits, error) {
	return &Commits{
		repositoryID: path,
		repo:         repo,
		refsIter:     refsIter,
		all:          all,
	}, nil
}

func (r *Commits) ReadHeader() ([]string, error) {
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

func (r *Commits) Read() (*util.Row, error) {
	if r.commitsIter == nil {
		r.commitsIter = r.GetIter()
	}

	commit, err := r.commitsIter.Next()
	if err != nil {
		return nil, err
	}

	return util.NewRow(util.Now(),
		r.repositoryID,
		commit.Hash.String(),
		commit.TreeHash.String(),
		commit.Message,
		commit.Author.Email,
		commit.Author.Name,
		commit.Author.When.Unix(),
		commit.Committer.Email,
		commit.Committer.Name,
		commit.Committer.When.Unix(),
	), nil
}

func (r *Commits) GetIter() object.CommitIter {
	if r.all {
		return &allCommitsIterator{
			repo:     r.repo,
			refsIter: r.refsIter,
		}
	}
	return &commitsIterator{
		repo:     r.repo,
		refsIter: r.refsIter,
	}
}

func (r *Commits) Close() error {
	if r.commitsIter != nil {
		r.commitsIter.Close()
	}
	if r.refsIter != nil {
		r.refsIter.Close()
	}
	return nil
}

type commitsIterator struct {
	repo     *git.Repository
	refsIter storer.ReferenceIter
}

func (iter *commitsIterator) Next() (*object.Commit, error) {
	ref, err := iter.refsIter.Next()
	if err != nil {
		return nil, err
	}

	refCommitHash, err := resolveRef(iter.repo, ref)
	if err != nil {
		return nil, err
	}

	return iter.repo.CommitObject(refCommitHash)
}

func (iter *commitsIterator) ForEach(cb func(*object.Commit) error) error {
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

func (iter *commitsIterator) Close() {}

type allCommitsIterator struct {
	repo        *git.Repository
	refsIter    storer.ReferenceIter
	commitsIter object.CommitIter
}

func (iter *allCommitsIterator) Next() (*object.Commit, error) {
	if iter.commitsIter == nil {
		ref, err := iter.refsIter.Next()
		if err != nil {
			return nil, err
		}

		refCommitHash, err := resolveRef(iter.repo, ref)
		if err != nil {
			return nil, err
		}

		iter.commitsIter, err = iter.repo.Log(&git.LogOptions{From: refCommitHash})
		if err != nil {
			return nil, err
		}
	}

	commit, err := iter.commitsIter.Next()
	if err == io.EOF {
		iter.commitsIter = nil
		return iter.Next()
	}
	return commit, err
}

// ForEach call the cb function for each reference contained on this iter until
// an error happens or the end of the iter is reached. If ErrStop is sent
// the iteration is stopped but no error is returned. The iterator is closed.
func (iter *allCommitsIterator) ForEach(cb func(*object.Commit) error) error {
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

func (iter *allCommitsIterator) Close() {}
