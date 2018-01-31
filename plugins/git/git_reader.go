package git

import (
	"fmt"

	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/plugins/git/blobs"
	"github.com/chrislusf/gleam/plugins/git/commits"
	"github.com/chrislusf/gleam/plugins/git/references"
	"github.com/chrislusf/gleam/plugins/git/repositories"
	"github.com/chrislusf/gleam/plugins/git/trees"
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

func Repositories(path string, partitionCount int) flow.Sourcer {
	return newGitSource("repositories", path, partitionCount)
}
func References(path string, partitionCount int) flow.Sourcer {
	return newGitSource("references", path, partitionCount)
}
func Commits(path string, partitionCount int) flow.Sourcer {
	return newGitSource("commits", path, partitionCount)
}
func Trees(path string, flag bool, partitionCount int) flow.Sourcer {
	return newGitSourceOptions("trees", path, flag, partitionCount)
}
func Blobs(path string, partitionCount int) flow.Sourcer {
	return newGitSource("blobs", path, partitionCount)
}

type reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
}

func (ds *shardInfo) NewReader(r *git.Repository, path string, flag bool) (reader, error) {
	switch ds.DataType {
	case "repositories":
		return repositories.NewReader(r, path)
	case "references":
		return references.NewReader(r, path)
	case "commits":
		return commits.NewReader(r, path)
	case "trees":
		return trees.NewReader(r, path, flag)
	case "blobs":
		return blobs.NewReader(r, path)
	}
	return nil, fmt.Errorf("unkown data type %q", ds.DataType)
}
