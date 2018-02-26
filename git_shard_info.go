package git

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/chrislusf/gleam/gio"
	"github.com/eiso/go-engine/readers"
	"github.com/pkg/errors"

	core "gopkg.in/src-d/core-retrieval.v0"
	"gopkg.in/src-d/core-retrieval.v0/repository"
	sivafs "gopkg.in/src-d/go-billy-siva.v4"
	"gopkg.in/src-d/go-billy.v4"
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
	return s.ReadSplit()
}

func (s *shardInfo) ReadSplit() error {
	log.Printf("reading %s from repo: %s", s.DataType, s.RepoPath)

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
			log.Printf("Could not open: %s - %s", s.RepoPath, err)
			return errors.Wrap(err, "could not open siva repository")
		}
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
		} else if err == readers.ErrRef {
			continue
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

func readSiva(origPath string) (*git.Repository, error) {

	local, copier, err := rootedTransactioner()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a rooted transactioner")
	}

	localPath := local.Join(
		fmt.Sprintf("%s_%s", origPath, strconv.FormatInt(time.Now().UnixNano(), 10)))
	localSivaPath := filepath.Join(localPath, "siva")
	localTmpPath := filepath.Join(localPath, "tmp")

	if err := copier.CopyFromRemote(origPath, localSivaPath, local); err != nil {
		return nil, errors.Wrap(err, "unable to copy from remote")
	}

	tmpFs, err := local.Chroot(localTmpPath)
	if err != nil {
		return nil, errors.Wrap(err, "unable to set the local path to the temp filesystem")
	}

	fs, err := sivafs.NewFilesystem(local, localSivaPath, tmpFs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a siva filesystem")
	}

	sto, err := filesystem.NewStorage(fs)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create a new storage backend")
	}

	repository, err := git.Open(sto, nil)
	if err != nil {
		return nil, errors.Wrap(err, "unable to open the git repository")
	}

	return repository, nil
}

// modified from: https://github.com/src-d/borges/blob/6a951a7fb9bcba73a996522a92bc506814b3b11c/cli/borges/packer.go#L83
func rootedTransactioner() (billy.Filesystem, repository.Copier, error) {
	tmpFs, err := core.TemporaryFilesystem().Chroot("siva-temp")
	if err != nil {
		return nil, nil, err
	}
	copier := repository.NewLocalCopier(osfs.New(""), 0)

	return tmpFs, copier, nil
}
