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

type GitReader interface {
	Read() (row *util.Row, err error)
	ReadHeader() (fieldNames []string, err error)
}

func Repositories(fsPath string, partitionCount int) *GitSource {
	return newGitSource("repositories", fsPath, partitionCount)
}
func References(fsPath string, partitionCount int) *GitSource {
	return newGitSource("references", fsPath, partitionCount)
}
func Commits(fsPath string, partitionCount int) *GitSource {
	return newGitSource("commits", fsPath, partitionCount)
}
func Trees(fsPath string, flag bool, partitionCount int) *GitSource {
	return newGitSourceOptions("trees", fsPath, flag, partitionCount)
}
func Blobs(fsPath string, partitionCount int) *GitSource {
	return newGitSource("blobs", fsPath, partitionCount)
}

func (ds *GitShardInfo) NewReader(r *git.Repository, path string, flag bool) (GitReader, error) {
	switch ds.GitDataType {
	case "repositories":
		return repositories.New(r, path), nil
	case "references":
		return references.New(r, path), nil
	case "commits":
		return commits.New(r, path), nil
	case "trees":
		return trees.New(r, path, flag), nil
	case "blobs":
		return blobs.New(r, path), nil
	}
	return nil, fmt.Errorf("Git data source '%s' is not defined.", ds.GitDataType)
}
