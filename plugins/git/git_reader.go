package git

import (
	"fmt"

	"github.com/chrislusf/gleam/plugins/git/blobs"
	"github.com/chrislusf/gleam/plugins/git/commits"
	"github.com/chrislusf/gleam/plugins/git/references"
	"github.com/chrislusf/gleam/plugins/git/repositories"
	"github.com/chrislusf/gleam/plugins/git/trees"
	"github.com/chrislusf/gleam/util"
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

type reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}

func (ds *shardInfo) NewReader(r *git.Repository, path string) (reader, error) {
	switch ds.DataType {
	case "repositories":
		return repositories.NewReader(r, path)
	case "references":
		return references.NewReader(r, path)
	case "commits":
		return commits.NewReader(r, path)
	case "trees":
		return trees.NewReader(r, path)
	case "blobs":
		return blobs.NewReader(r, path)
	}
	return nil, fmt.Errorf("unkown data type %q", ds.DataType)
}
