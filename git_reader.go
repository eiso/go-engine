package git

import (
	"fmt"

	"github.com/chrislusf/gleam/util"
	"github.com/eiso/go-engine/readers"
	git "gopkg.in/src-d/go-git.v4"
)

func Repositories(path string, partitionCount int) *sourceRepositories {
	return newGitRepositories(path, partitionCount)
}

type reader interface {
	Read() (*util.Row, error)
	ReadHeader() ([]string, error)
	Close() error
}

func (ds *shardInfo) NewReader(r *git.Repository, path string, flag bool) (reader, error) {
	// .Repositories()
	if ds.DataType == "repositories" {
		repoReader, err := readers.NewRepositories(r, path)
		if err != nil {
			repoReader.Close()
			return nil, err
		}
		repoReader.Close()
		return repoReader, nil
	}

	// .References()
	refsReader, err := readers.NewReferences(r, path, ds.FilterRefs)
	if err != nil {
		refsReader.Close()
		return nil, err
	}

	if ds.DataType == "references" {
		return refsReader, nil
	}

	// .Commits()
	refsIter, err := refsReader.GetIter()
	if err != nil {
		refsIter.Close()
		return nil, err
	}

	commitsReader, err := readers.NewCommits(r, path, refsIter, ds.AllCommits)
	if err != nil {
		refsReader.Close()
		refsIter.Close()
		commitsReader.Close()
		return nil, err
	}

	if ds.DataType == "commits" {
		return commitsReader, nil
	}

	if ds.DataType == "trees" {
		treesReader, err := readers.NewTrees(r, path, commitsReader.GetIter())
		if err != nil {
			return nil, err
		}
		return treesReader, nil
	} else if ds.DataType == "blobs" {
		blobsReader, err := readers.NewBlobs(r, path, commitsReader.GetIter())
		if err != nil {
			return nil, err
		}
		return blobsReader, nil
	}

	return nil, fmt.Errorf("unkown data type %q", ds.DataType)
}
