package engine

import (
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"

	"github.com/chrislusf/gleam/filesystem"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/pb"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"
)

type GitSource struct {
	folder         string
	fileBaseName   string
	hasWildcard    bool
	path           string
	hasHeader      bool
	partitionCount int
	dataType       string
	nestedSource   nestedSource

	prefix string
}

type pipeline interface {
	Commits() *GitSource
	References() *GitSource
	Trees() *GitSource
	flow.Sourcer
}

type nestedSource map[string]Options

type Options struct {
	Filter  map[int][]string
	Reverse bool
}

// New creates a GitSource based on a path.
func newGitSource(dataType, fsPath string, partitionCount int) *GitSource {
	base := filepath.Base(fsPath)

	return &GitSource{
		partitionCount: partitionCount,
		dataType:       dataType,
		prefix:         dataType,
		hasHeader:      true,
		folder:         filepath.Dir(fsPath),
		fileBaseName:   base,
		path:           fsPath,
		hasWildcard:    strings.Contains(base, "**"),
		nestedSource:   nestedSource{},
	}
}

func (s *GitSource) Commits(options ...func(*Options)) *GitSource {
	opts := Options{}
	for _, option := range options {
		option(&opts)
	}

	s.nestedSource["commits"] = opts
	return s
}

func (s *GitSource) References(options ...func(*Options)) *GitSource {
	opts := Options{}
	for _, option := range options {
		option(&opts)
	}

	s.nestedSource["references"] = opts
	return s
}

func (s *GitSource) Trees(options ...func(*Options)) *GitSource {
	opts := Options{}
	for _, option := range options {
		option(&opts)
	}

	s.nestedSource["trees"] = opts
	return s
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *GitSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.prefix, s.partitionCount).Map(s.prefix+".Read", registeredMapperReadShard)
}

func (s *GitSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix+"."+s.fileBaseName, func(out io.Writer, stats *pb.InstructionStat) error {
		stats.InputCounter++
		defer func() { log.Printf("Git repos: %d", stats.OutputCounter) }()

		if s.hasWildcard {
			return s.gitRepos(s.folder, out, stats)
		}

		if !filesystem.IsDir(s.path) {
			return errors.New("source can't be be a file")
		}

		if !s.isRepo(s.path) {
			return s.gitRepos(s.path, out, stats)
		}

		stats.OutputCounter++
		s := &shardInfo{
			RepoPath:     s.path,
			DataType:     s.dataType,
			HasHeader:    s.hasHeader,
			NestedSource: s.nestedSource,
		}
		b, err := s.encode()
		if err != nil {
			// TODO: improve error handling.
			log.Fatalf("could not encocde shard info: %v", err)
		}
		return util.NewRow(util.Now(), b).WriteTo(out)
	})
}

// Find all repositories in the directory
func (s *GitSource) gitRepos(folder string, out io.Writer, stats *pb.InstructionStat) error {
	virtualFiles, err := filesystem.List(folder)
	if err != nil {
		return fmt.Errorf("Failed to list folder %s: %v", folder, err)
	}

	for _, vf := range virtualFiles {
		if !filesystem.IsDir(vf.Location) {
			continue
		}

		if !s.isRepo(vf.Location) {
			err = s.gitRepos(vf.Location, out, stats)
			if err != nil {
				return err
			}
			continue
		}

		stats.OutputCounter++
		s := &shardInfo{
			RepoPath:     vf.Location,
			DataType:     s.dataType,
			HasHeader:    s.hasHeader,
			NestedSource: s.nestedSource,
		}

		b, err := s.encode()
		if err != nil {
			return errors.Wrap(err, "could not encode shard info")
		}
		if err := util.NewRow(util.Now(), b).WriteTo(out); err != nil {
			return errors.Wrap(err, "could not encode row")
		}
	}

	return nil
}

func (s *GitSource) isRepo(path string) bool {
	p := filepath.Join(path, ".git")
	_, err := filesystem.Open(p)
	if err != nil {
		return false
	}
	return true
}
