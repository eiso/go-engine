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
)

type GitSource struct {
	folder         string
	fileBaseName   string
	hasWildcard    bool
	Path           string
	HasHeader      bool
	PartitionCount int
	GitDataType    string
	Fields         []string

	prefix string
}

// Generate generates data shard info,
// partitions them via round robin,
// and reads each shard on each executor
func (s *GitSource) Generate(f *flow.Flow) *flow.Dataset {
	return s.genShardInfos(f).RoundRobin(s.prefix, s.PartitionCount).Map(s.prefix+".Read", registeredMapperReadShard)
}

// TODO adjust GitSource api to denote which data source can support columnar reads
// Select selects fields that can be pushed down to data sources supporting columnar reads
func (q *GitSource) Select(fields ...string) *GitSource {
	q.Fields = fields
	return q
}

// New creates a GitSource based on a path.
func newGitSource(gitDataType, fileOrPattern string, partitionCount int) *GitSource {

	s := &GitSource{
		PartitionCount: partitionCount,
		GitDataType:    gitDataType,
		prefix:         gitDataType,
		HasHeader:      true,
	}

	var err error
	fileOrPattern, err = filepath.Abs(fileOrPattern)
	if err != nil {
		log.Fatalf("path \"%s\" not found: %v", fileOrPattern, err)
	}

	s.folder = filepath.Dir(fileOrPattern)
	s.fileBaseName = filepath.Base(fileOrPattern)
	s.Path = fileOrPattern

	if strings.Contains(s.fileBaseName, "**") {
		s.hasWildcard = true
	}

	return s
}

func (s *GitSource) genShardInfos(f *flow.Flow) *flow.Dataset {
	return f.Source(s.prefix+"."+s.fileBaseName, func(writer io.Writer, stats *pb.InstructionStat) error {
		stats.InputCounter++
		if !s.hasWildcard && filesystem.IsDir(s.Path) && filesystem.IsDir(filepath.Join(s.Path, "/.git/")) {
			stats.OutputCounter++
			util.NewRow(util.Now(), encodeShardInfo(&GitShardInfo{
				RepoPath:    s.Path,
				GitDataType: s.GitDataType,
				HasHeader:   s.HasHeader,
				Fields:      s.Fields,
			})).WriteTo(writer)
		} else {
			var e []*filesystem.FileLocation

			_, err := s.gitRepos(s.folder, e, writer, stats)
			if err != nil {
				return fmt.Errorf("Failed to list folder %s: %v", s.folder, err)
			}
		}
		log.Printf("Git repos: %d", stats.OutputCounter)
		return nil
	})
}

func (s *GitSource) gitRepos(folder string, v []*filesystem.FileLocation, writer io.Writer, stats *pb.InstructionStat) ([]*filesystem.FileLocation, error) {
	virtualFiles, err := filesystem.List(folder)
	if err != nil {
		return nil, fmt.Errorf("Failed to list folder %s: %v", folder, err)
	}

	for _, vf := range virtualFiles {
		if filesystem.IsDir(vf.Location) {
			if filesystem.IsDir(filepath.Join(vf.Location, "/.git/")) {
				stats.OutputCounter++
				util.NewRow(util.Now(), encodeShardInfo(&GitShardInfo{
					RepoPath:    vf.Location,
					GitDataType: s.GitDataType,
					HasHeader:   s.HasHeader,
					Fields:      s.Fields,
				})).WriteTo(writer)

				continue
			} else {
				v = append(v, vf)
				s.gitRepos(vf.Location, v, writer, stats)
			}
		}
	}

	return v, nil
}
