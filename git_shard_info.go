package git

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/chrislusf/gleam/gio"
	"github.com/eiso/go-engine/readers"
	"github.com/pkg/errors"

	sivafs "github.com/eiso/go-billy-siva"
	"gopkg.in/src-d/go-billy.v4/memfs"
	"gopkg.in/src-d/go-billy.v4/osfs"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/storage/filesystem"
)

var regMapperReadShard = gio.RegisterMapper(newReadShard)

func init() {
	gob.Register(shardInfo{})
}

type shardInfo struct {
	// these fields are exported so gob encoding can see them.
	Config     map[string]string
	RepoPath   string
	RepoType   string
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

	err := s.ReadSplit()
	if err != nil {
		log.Printf("newReadShard error: %s", err)
	}
	return err
}

func (s *shardInfo) ReadSplit() error {
	log.Printf("started reading %s from: %s", s.DataType, s.RepoPath)

	var repo *git.Repository
	var err error
	if s.RepoType == "standard" {
		repo, err = git.PlainOpen(s.RepoPath)
		if err != nil {
			return errors.Wrap(err, "could not open git repository")
		}
	} else if s.RepoType == "siva" {
		repo, err = readSiva(s.RepoPath)
		if err != nil {
			return errors.Wrap(err, "could not open siva repository")
		}
	}

	reader, err := s.NewReader(repo, s.RepoPath, false)
	if err != nil {
		return errors.Wrapf(err, "could not read repository %s", s.RepoPath)
	}
	defer reader.Close()

	if s.HasHeader {
		if _, err := reader.ReadHeader(); err != nil {
			return errors.Wrap(err, "could not read headers")
		}
	}

	for {
		row, err := reader.Read()
		if err == io.EOF {
			log.Printf("finished reading %s: %s", s.DataType, s.RepoPath)
			return nil
		} else if err == readers.ErrRef || err == readers.ErrObj {
			continue
		} else if err != nil {
			return errors.Wrap(err, "could not get next file")
		}
		// Writing to stdout is how agents communicate.
		if err := row.WriteTo(os.Stdout); err != nil {
			return errors.Wrap(err, "could not write row to stdout")
		}
	}
}

func readSiva(origPath string) (*git.Repository, error) {
	local := osfs.New(filepath.Dir(origPath))
	tmpFs := memfs.New()

	origPath = filepath.Base(origPath)

	fs, err := sivafs.NewFilesystem(local, origPath, tmpFs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a siva filesystem")
	}

	sto, err := filesystem.NewStorage(fs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a new storage backend")
	}

	repository, err := git.Open(sto, tmpFs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open the git repository")
	}
	return repository, nil
}
