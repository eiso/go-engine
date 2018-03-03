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
	if ds.DataType == "repositories" {
		repoReader, err := readers.NewRepositories(r, path)
		if err != nil {
			return nil, err
		}
		return repoReader, nil
	}

	refsReader, err := readers.NewReferences(r, path, ds.FilterRefs)
	if err != nil {
		return nil, err
	}

	if ds.DataType == "references" {
		return refsReader, nil
	}

	refs, err := refsReader.GetIter()
	if err != nil {
		refsReader.Close()
		return nil, err
	}

	commitsReader, err := readers.NewCommits(r, path, refs, ds.AllCommits)
	if err != nil {
		refs.Close()
		refsReader.Close()
		return nil, err
	}

	if ds.DataType == "commits" {
		return commitsReader, nil
	}
	closer := func() {
		commitsReader.Close()
		refs.Close()
		refsReader.Close()
	}

	if ds.DataType == "trees" {
		treesReader, err := readers.NewTrees(r, path, commitsReader.GetIter())
		if err != nil {
			closer()
			return nil, err
		}
		return treesReader, nil
	} else if ds.DataType == "blobs" {
		blobsReader, err := readers.NewBlobs(r, path, commitsReader.GetIter())
		if err != nil {
			closer()
			return nil, err
		}
		return blobsReader, nil
	}
	closer()
	return nil, fmt.Errorf("unkown data type %q", ds.DataType)
}
