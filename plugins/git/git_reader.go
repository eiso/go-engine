package git

import (
	"fmt"

	"github.com/chrislusf/gleam/plugins/git/commits"
	"github.com/chrislusf/gleam/plugins/git/global"
	"github.com/chrislusf/gleam/plugins/git/references"
	"github.com/chrislusf/gleam/plugins/git/repositories"
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
func Blobs(path string, partitionCount int) *GitSource {
	return newGitSource("blobs", path, partitionCount)
}

func (ds *shardInfo) NewReader(source string, r *git.Repository, path string, options Options, readers map[string]global.Reader) (global.Reader, error) {
	var reader global.Reader

	switch source {
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
	case "default":
		return nil, fmt.Errorf("%s is not an implemented source", source)
	}
	return reader, nil
}
