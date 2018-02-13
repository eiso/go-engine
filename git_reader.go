package engine

import (
	"fmt"

	"github.com/eiso/go-engine/source"
	"github.com/eiso/go-engine/source/commits"
	"github.com/eiso/go-engine/source/references"
	"github.com/eiso/go-engine/source/repositories"
	"github.com/eiso/go-engine/source/trees"
	git "gopkg.in/src-d/go-git.v4"
)

func Repositories(path string, partitionCount int) *GitSource {
	return newGitSource("repositories", path, partitionCount)
}
func References(path string, partitionCount int) *GitSource {
	return newGitSource("references", path, partitionCount)
}
func Commits(path string, partitionCount int) *GitSource {
	return newGitSource("commits", path, partitionCount)
}
func Trees(path string, partitionCount int) *GitSource {
	return newGitSource("trees", path, partitionCount)
}

func (ds *shardInfo) NewReader(src string, r *git.Repository, path string, options Options, readers map[string]source.Reader) (source.Reader, error) {
	var reader source.Reader

	switch src {
	case "repositories":
		opts, err := repositories.NewOptions(options.Filter, options.Reverse)
		if err != nil {
			return nil, err
		}
		reader, err = repositories.NewReader(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "references":
		opts, err := references.NewOptions(options.Filter, options.Reverse)
		if err != nil {
			return nil, err
		}
		reader, err = references.NewReader(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "commits":
		opts, err := commits.NewOptions(options.Filter, options.Reverse)
		if err != nil {
			return nil, err
		}
		reader, err = commits.NewReader(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "trees":
		opts, err := trees.NewOptions(options.Filter, options.Reverse)
		if err != nil {
			return nil, err
		}
		reader, err = trees.NewReader(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "default":
		return nil, fmt.Errorf("%s is not an implemented source", src)
	}
	return reader, nil
}
