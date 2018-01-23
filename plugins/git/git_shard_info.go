package git

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"

	"github.com/chrislusf/gleam/gio"
	git "gopkg.in/src-d/go-git.v4"
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

func (ds *GitShardInfo) ReadSplit() error {

	println("opening repo", ds.RepoPath)

	r, err := git.PlainOpen(ds.RepoPath)
	if err != nil {
		return err
	}

	reader, err := ds.NewReader(r)
	if err != nil {
		return fmt.Errorf("Failed to read repository %s: %v", ds.RepoPath, err)
	}
	if ds.HasHeader {
		reader.ReadHeader()
	}

	switch ds.GitDataType {

	case "repositories":
		row, err := reader.Read()
		if err != nil {
			break
		}
		row.WriteTo(os.Stdout)
	case "references":
		for {
			row, err := reader.Read()
			if err != nil {
				break
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
