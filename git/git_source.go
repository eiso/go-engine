package git

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

type baseSource struct {
	partitionCount int
	// where to read from
	folder       string
	fileBaseName string
	hasWildcard  bool
	path         string
	hasHeader    bool
	prefix       string

	// FIXME most probably it shouldn't be here
	FilterRefs []string
	allCommits bool
}

type sourceRepositories struct {
	baseSource
}

type sourceReferences struct {
	baseSource
}

type sourceCommits struct {
	baseSource
}

type sourceTrees struct {
	baseSource
}

type sourceBlobs struct {
	baseSource
}

func newGitRepositories(fsPath string, partitionCount int) *sourceRepositories {
	base := filepath.Base(fsPath)

	return &sourceRepositories{
		baseSource: baseSource{
			partitionCount: partitionCount,
			hasHeader:      true,
			folder:         filepath.Dir(fsPath),
			fileBaseName:   base,
			path:           fsPath,
			hasWildcard:    strings.Contains(base, "**"),
			prefix:         "repositories",
		},
	}
}

// Find all repositories in the directory
func (s *baseSource) gitRepos(folder string, out io.Writer, stats *pb.InstructionStat) error {
	virtualFiles, err := filesystem.List(folder)
	if err != nil {
		return fmt.Errorf("Failed to list folder %s: %v", folder, err)
	}

	for _, vf := range virtualFiles {
		if !filesystem.IsDir(vf.Location) || strings.Contains(vf.Location, ".") {
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
			RepoPath:   vf.Location,
			DataType:   s.prefix,
			HasHeader:  s.hasHeader,
			FilterRefs: s.FilterRefs,
			AllCommits: s.allCommits,
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

func (s *baseSource) isRepo(path string) bool {
	p := filepath.Join(path, ".git")
	_, err := filesystem.Open(p)
	if err != nil {
		return false
	}
	return true
}

func (s *baseSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.prefix, s.partitionCount).Map(s.prefix+".Read", regMapperReadShard)
}

func (s *baseSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix+"."+s.path, func(out io.Writer, stats *pb.InstructionStat) error {
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
			RepoPath:   s.path,
			DataType:   s.prefix,
			HasHeader:  s.hasHeader,
			FilterRefs: s.FilterRefs,
			AllCommits: s.allCommits,
		}
		b, err := s.encode()
		if err != nil {
			// TODO: improve error handling.
			log.Fatalf("could not encode shard info: %v", err)
		}
		return util.NewRow(util.Now(), b).WriteTo(out)
	})
}

func (s *sourceRepositories) References() *sourceReferences {
	newSource := s.baseSource
	newSource.prefix = "references"
	return &sourceReferences{
		baseSource: newSource,
	}
}

func (s *sourceReferences) Filter(refs ...string) *sourceReferences {
	s.baseSource.FilterRefs = refs
	return s
}

func (s *sourceReferences) Commits() *sourceCommits {
	newSource := s.baseSource
	newSource.prefix = "commits"
	return &sourceCommits{
		baseSource: newSource,
	}
}

func (s *sourceReferences) AllReferenceCommits() *sourceCommits {
	newSource := s.baseSource
	newSource.prefix = "commits"
	newSource.allCommits = true
	return &sourceCommits{
		baseSource: newSource,
	}
}

func (s *sourceCommits) Trees() *sourceTrees {
	newSource := s.baseSource
	newSource.prefix = "trees"
	return &sourceTrees{
		baseSource: newSource,
	}
}

func (s *sourceTrees) Blobs() *sourceBlobs {
	newSource := s.baseSource
	newSource.prefix = "blobs"
	return &sourceBlobs{
		baseSource: newSource,
	}
}
