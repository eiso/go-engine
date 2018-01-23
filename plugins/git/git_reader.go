package git

import (
	"fmt"

	"github.com/chrislusf/gleam/plugins/git/blobs"
	"github.com/chrislusf/gleam/plugins/git/references"
	"github.com/chrislusf/gleam/plugins/git/repositories"
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
)

type GitReader interface {
	Read() (row *util.Row, err error)
	ReadHeader() (fieldNames []string, err error)
}

func Repositories(fileOrPattern string, partitionCount int) *GitSource {
	return newGitSource("repositories", fileOrPattern, partitionCount)
}
func References(fileOrPattern string, partitionCount int) *GitSource {
	return newGitSource("references", fileOrPattern, partitionCount)
}

/*
func Commits(fileOrPattern string, partitionCount int) *GitSource {
	return newGitSource("commits", fileOrPattern, partitionCount)
}
func TreeEntries(fileOrPattern string, partitionCount int) *GitSource {
	return newGitSource("treeEntries", fileOrPattern, partitionCount)
}*/
func Blobs(fileOrPattern string, partitionCount int) *GitSource {
	return newGitSource("blobs", fileOrPattern, partitionCount)
}

func (ds *GitShardInfo) NewReader(r *git.Repository) (GitReader, error) {
	switch ds.GitDataType {
	case "repositories":
		return repositories.New(r), nil
	case "references":
		return references.New(r), nil
	/*case "commits":
		return commits.New(r), nil
	case "treeEntries":
		return treeEntries.New(r), nil*/
	case "blobs":
		return blobs.New(r), nil
	}
	return nil, fmt.Errorf("Git data source '%s' is not defined.", ds.GitDataType)
}
