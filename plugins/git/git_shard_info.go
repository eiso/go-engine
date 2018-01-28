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

// Pipeline strings together the given exec.Cmd commands in a similar fashion
// to the Unix pipeline.  Each command's standard output is connected to the
// standard input of the next command, and the output of the final command in
// the pipeline is returned, along with the collected standard error of all
// commands and the first error found (if any).
//
// To provide input to the pipeline, assign an io.Reader to the first's Stdin.
func Pipeline(cmds ...*exec.Cmd) (pipeLineOutput, collectedStandardError []byte, pipeLineError error) {
	// Require at least one command
	if len(cmds) < 1 {
		return nil, nil, nil
	}

	// Collect the output from the command(s)
	var output bytes.Buffer
	var stderr bytes.Buffer

	last := len(cmds) - 1
	for i, cmd := range cmds[:last] {
		// Connect each command's stdin to the previous command's stdout
		c, err := cmd.StdoutPipe()
		cmds[i+1].Stdin = c
		if err != nil {
			return nil, nil, err
		}
		// Connect each command's stderr to a buffer
		cmd.Stderr = &stderr
	}

	// Connect the output and error for the last command
	cmds[last].Stdout, cmds[last].Stderr = &output, &stderr

	// Start each command
	for _, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	// Wait for each command to complete
	for _, cmd := range cmds {
		if err := cmd.Wait(); err != nil {
			return output.Bytes(), stderr.Bytes(), err
		}
	}

	// Return the pipeline output and the collected standard error
	return output.Bytes(), stderr.Bytes(), nil
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
	cmd1 := exec.Command(cmdName, cmdArgs...)
	cmd1.Dir = path

	//cmdName2 := "cut"
	//cmdArgs2 := []string{"-d", "\" \"", "-f", "1"}
	//cmd2 := exec.Command(cmdName2, cmdArgs2...)

	//cmd3 := exec.Command("uniq")

	output, stderr, err := Pipeline(cmd1)
	if err != nil {
		return fmt.Errorf("pipeline failed: %s", err)
	}
	if len(stderr) > 0 {
		return fmt.Errorf("pipeline failed, stderr: %s", stderr)
	}

	s := strings.Fields(string(output))
	log.Printf("%d", len(s))
	o := removeDuplicates(s)
	log.Printf("%d", len(o))

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
		os.Exit(1)
	default:
		for {
			row, err := reader.Read()
			if err != nil {
				break
			}
			if row == nil {
				continue
			}
			row.WriteTo(os.Stdout)
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
