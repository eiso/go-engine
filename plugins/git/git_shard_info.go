package git

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

type GitShardInfo struct {
	Config      map[string]string
	RepoPath    string
	GitDataType string
	HasHeader   bool
	Fields      []string
}

var (
	registeredMapperReadShard = gio.RegisterMapper(readShard)
)

func init() {
	gob.Register(GitShardInfo{})
}

func readShard(row []interface{}) error {
	encodedShardInfo := row[0].([]byte)
	return decodeShardInfo(encodedShardInfo).ReadSplit()
}

func removeDuplicates(a []string) []string {
	result := []string{}
	seen := map[string]string{}
	for _, val := range a {
		if _, ok := seen[val]; !ok {
			result = append(result, val)
			seen[val] = val
		}
	}
	return result
}

func getRefChildren(path string, refHash string, refName string) error {
	cmdName := "git"
	cmdArgs := []string{"rev-list", "--children", refHash}
	cmd := exec.Command(cmdName, cmdArgs...)
	cmd.Dir = path

	output, err := cmd.Output()
	if err != nil {
		fmt.Errorf("There was an error running %s %s: %s", cmdName, cmdArgs, err)
	}

	s := strings.Fields(string(output))
	o := removeDuplicates(s)
	for _, commitHash := range o {
		row := util.NewRow(util.Now(), path, refHash, refName, commitHash)
		row.WriteTo(os.Stdout)
	}

	return nil
}

func (ds *GitShardInfo) ReadSplit() error {

	println("opening repo", ds.RepoPath)

	r, err := git.PlainOpen(ds.RepoPath)
	if err != nil {
		return err
	}

	reader, err := ds.NewReader(r, ds.RepoPath)
	if err != nil {
		return fmt.Errorf("Failed to read repository %s: %v", ds.RepoPath, err)
	}
	if ds.HasHeader {
		reader.ReadHeader()
	}

	switch {
	case ds.GitDataType == "references":
		refs, _ := r.References()
		refs.ForEach(func(ref *plumbing.Reference) error {
			if ref.Hash().IsZero() {
				return nil
			}
			err := getRefChildren(ds.RepoPath, ref.Hash().String(), ref.Name().String())
			if err != nil {
				return err
			}
			return nil
		})
	default:
		for {
			row, err := reader.Read()
			if err != nil && row == nil {
				break
			}
			if err == nil && row == nil {
				continue
			}

			row.WriteTo(os.Stdout)

			if err != nil && row != nil {
				break
			}
		}
	}

	return err
}

func decodeShardInfo(encodedShardInfo []byte) *GitShardInfo {
	network := bytes.NewBuffer(encodedShardInfo)
	dec := gob.NewDecoder(network)
	var p GitShardInfo
	if err := dec.Decode(&p); err != nil {
		log.Fatal("decode shard info", err)
	}
	return &p
}

func encodeShardInfo(shardInfo *GitShardInfo) []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	if err := enc.Encode(shardInfo); err != nil {
		log.Fatal("encode shard info:", err)
	}
	return network.Bytes()
}
