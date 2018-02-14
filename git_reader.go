package engine

import (
	"fmt"

	"github.com/eiso/go-engine/options"
	"github.com/eiso/go-engine/source"
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

func (ds *shardInfo) NewReader(src string, r *git.Repository, path string, opts *options.Config, readers map[string]source.SourceReaders) (source.SourceReaders, error) {
	var reader source.SourceReaders
	var err error

	switch src {
	case "repositories":
		reader, err = source.NewRepositories(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "references":
		reader, err = source.NewReferences(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "commits":
		reader, err = source.NewCommits(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "trees":
		reader, err = source.NewTrees(r, path, opts, readers)
		if err != nil {
			return nil, err
		}
	case "default":
		return nil, fmt.Errorf("%s is not an implemented source", src)
	}
	return reader, nil
}
