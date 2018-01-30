package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"

	"gopkg.in/bblfsh/client-go.v2"
	protocol "gopkg.in/bblfsh/sdk.v1/protocol"
	enry "gopkg.in/src-d/enry.v1"
	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")

	regKeyRefHash    = gio.RegisterMapper(flipKey(3))
	regKeyCommitHash = gio.RegisterMapper(flipKey(1))

	regReadBlob         = gio.RegisterMapper(readBlob)
	regClassifyLanguage = gio.RegisterMapper(classifyLanguage(2, 6))
	regExtractUAST      = gio.RegisterMapper(extractUAST)
)

func main() {
	gio.Init()

	f := flow.New("Git pipeline")
	path := "/home/mthek/projects/enginerepos-srcd/**"

	repos := f.Read(git.Repositories(path, 1))
	refs := f.Read(git.References(path, 1)) //.Pipe("grep", "grep -e 'refs/heads/master'")
	commits := f.Read(git.Commits(path, 1)).
		Map("KeyCommitHash", regKeyCommitHash)
	trees := f.Read(git.Trees(path, 1)).
		Map("KeyTreeHash", regKeyCommitHash)

	joinA := refs.JoinByKey("Refs & Repos", repos).
		Map("KeyRefHash", regKeyRefHash)
	joinB := joinA.LeftOuterJoinByKey("Refs & Commits", commits)
	joinC := joinB.JoinByKey("Trees & Refs & Commits", trees)

	p := joinC.OutputRow(func(row *util.Row) error {
		fmt.Printf("\n\n%v\t", row.K[0])
		i := 0
		for _, v := range row.V {
			fmt.Printf("%v\t", v)
			i++
		}
		return nil
	})

	if *isDistributed {
		p.Run(distributed.Option())
	} else if *isDockerCluster {
		p.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		p.Run()
	}
}

func flipKey(newKeyIdx int) gio.Mapper {
	return func(x []interface{}) error {
		newKey := make([]interface{}, 1)
		newKey[0] = x[newKeyIdx]
		row := x[:newKeyIdx]
		if len(x) > newKeyIdx+1 {
			row = append(row, x[newKeyIdx+1:]...)
		}
		row = append(newKey, row...)
		gio.Emit(row...)
		return nil
	}
}

//TODO: Update to new index approach
func readBlob(x []interface{}) error {
	repoPath := gio.ToString(x[1])
	blobHash := plumbing.NewHash(gio.ToString(x[5]))
	contents := []byte("")

	if !blobHash.IsZero() {
		r, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return err
		}

		blob, err := r.BlobObject(blobHash)
		if err != nil {
			return err
		}

		reader, err := blob.Reader()
		if err != nil {
			return err
		}

		contents, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
	}

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], contents)
	return nil
}

func classifyLanguage(fileNameIdx int, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[fileNameIdx])
		content := x[contentIdx].([]byte)
		lang := enry.GetLanguage(filename, content)
		gio.Emit(append(x, lang)...)
		return nil
	}
}

//TODO: Update to new index approach
func extractUAST(x []interface{}) error {
	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		panic(err)
	}

	blob := gio.ToString(x[4])

	res, err := client.NewParseRequest().Language("python").Content(blob).Do()
	if err != nil {
		panic(err)
	}

	if res.Response.Status == protocol.Fatal {
		res.Language = ""
	}

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], res.Language)
	return nil
}

func truncateString(v interface{}, num int) interface{} {
	b := v
	if v, ok := v.(string); ok {
		if len(v) > num {
			b = v[0:num]
		}
	}
	return b
}
