package engine

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/gio"
	"github.com/eiso/go-engine/options"
	"github.com/eiso/go-engine/source"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
)

type shardInfo struct {
	// these fields are exported so gob encoding can see them.
	Config       map[string]string
	RepoPath     string
	DataType     string
	HasHeader    bool
	Fields       []string
	NestedSource nestedSource
}

var registeredMapperReadShard = gio.RegisterMapper(readShard)

func init() {
	gob.Register(shardInfo{})
}

func readShard(row []interface{}) error {
	var s shardInfo
	if err := s.decode(gio.ToBytes(row[0])); err != nil {
		return err
	}
	return s.ReadSplit()
}

func removeDuplicates(vs []string) []string {
	result := make([]string, 0, len(vs))
	seen := make(map[string]string, len(vs))
	for _, v := range vs {
		if _, ok := seen[v]; !ok {
			result = append(result, v)
			seen[v] = v
		}
	}
	return result
}

func (s *shardInfo) ReadSplit() error {
	log.Printf("reading %s from repo: %s", s.DataType, s.RepoPath)

	repo, err := git.PlainOpen(s.RepoPath)
	if err != nil {
		return errors.Wrap(err, "could not open repo")
	}

	rs := make(map[string]source.SourceReaders)
	temp := make(map[string]source.SourceReaders)

	//TODO Need to still add options to repositories base source
	emptyOptions := &options.Config{}
	reposReader, err := s.NewReader(s.DataType, repo, s.RepoPath, emptyOptions, nil)
	if err != nil {
		return errors.Wrapf(err, "could not read repository %s", s.RepoPath)
	}

	temp[s.DataType] = reposReader
	rs[s.DataType] = reposReader

	// the overhead of building the readers twice is limited
	// but necessary for chained itteration
	for source, options := range s.NestedSource {
		r, err := s.NewReader(source, repo, s.RepoPath, &options, nil)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return errors.Wrap(err, "could not read source")
		}
		temp[source] = r
	}

	const (
		repositories = iota
		references
		commits
		trees
		blobs
	)

	deepestSource := repositories
	var nameDeepestSource string

	for source, options := range s.NestedSource {
		r, err := s.NewReader(source, repo, s.RepoPath, &options, temp)
		if err != nil {
			return errors.Wrap(err, "could not read references")
		}
		rs[source] = r

		switch source {
		case "repositories":
			if deepestSource < repositories {
				deepestSource = repositories
				nameDeepestSource = source
			}
		case "references":
			if deepestSource < references {
				deepestSource = references
				nameDeepestSource = source
			}
		case "commits":
			if deepestSource < commits {
				deepestSource = commits
				nameDeepestSource = source
			}
		case "trees":
			if deepestSource < trees {
				deepestSource = trees
				nameDeepestSource = source
			}
		case "blobs":
			if deepestSource < blobs {
				deepestSource = blobs
				nameDeepestSource = source
			}
		}
	}

	for {
		row, err := rs[nameDeepestSource].Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.Wrap(err, "unable to itterate further")
		}
		if err := row.WriteTo(os.Stdout); err != nil {
			return errors.Wrap(err, "could not write row to stdout")
		}
	}

	return nil
}

func (s *shardInfo) decode(b []byte) error {
	dec := gob.NewDecoder(bytes.NewReader(b))
	return dec.Decode(s)
}

func (s *shardInfo) encode() ([]byte, error) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	if err := enc.Encode(s); err != nil {
		return nil, errors.Wrap(err, "could not encode shard info")
	}
	return b.Bytes(), nil
}
