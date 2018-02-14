package git

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"

	"github.com/chrislusf/gleam/gio"
	"github.com/pkg/errors"
	git "gopkg.in/src-d/go-git.v4"
)

var regMapperReadShard = gio.RegisterMapper(newReadShard)

func init() {
	gob.Register(shardInfo{})
}

type shardInfo struct {
	// these fields are exported so gob encoding can see them.
	Config     map[string]string
	RepoPath   string
	DataType   string
	HasHeader  bool
	FilterRefs []string
	AllCommits bool
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

func newReadShard(row []interface{}) error {
	var s shardInfo
	if err := s.decode(gio.ToBytes(row[0])); err != nil {
		return err
	}
	return s.ReadSplit()
}

func (s *shardInfo) ReadSplit() error {
	log.Printf("reading %s from repo: %s", s.DataType, s.RepoPath)

	repo, err := git.PlainOpen(s.RepoPath)
	if err != nil {
		return errors.Wrap(err, "could not open repo")
	}

	reader, err := s.NewReader(repo, s.RepoPath, false)
	if err != nil {
		log.Println("err", err)
		return errors.Wrapf(err, "could not read repository %s", s.RepoPath)
	}
	if s.HasHeader {
		if _, err := reader.ReadHeader(); err != nil {
			return errors.Wrap(err, "could not read headers")
		}
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return errors.Wrap(err, "could not get next file")
		}

		// Writing to stdout is how agents communicate.
		if err := row.WriteTo(os.Stdout); err != nil {
			return errors.Wrap(err, "could not write row to stdout")
		}
	}
	return nil
}
