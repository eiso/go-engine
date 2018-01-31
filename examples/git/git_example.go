package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"
	"github.com/pkg/errors"

	enry "gopkg.in/src-d/enry.v1"
	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

func main() {
	var (
		isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
		isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")

		regKeyRefHash    = gio.RegisterMapper(flipKey(3))
		regKeyCommitHash = gio.RegisterMapper(flipKey(1))
	)
	gio.Init()

	var path = "."
	if args := flag.Args(); len(args) > 0 {
		path = args[0]
	}
	log.Printf("analyzing %s", path)

	start := time.Now()

	f := flow.New("Git pipeline")

	repos := f.Read(git.Repositories(path, 1))
	refs := f.Read(git.References(path, 1))
	commits := f.Read(git.Commits(path, 1)).
		Map("KeyCommitHash", regKeyCommitHash)
	trees := f.Read(git.Trees(path, false, 1)).
		Map("KeyTreeHash", regKeyCommitHash)

	p := refs.
		JoinByKey("Refs & Repos", repos).
		Map("KeyRefHash", regKeyRefHash).
		LeftOuterJoinByKey("Refs & Commits", commits).
		JoinByKey("Trees & Refs & Commits", trees).
		OutputRow(printRow)

	var opts []flow.FlowOption
	switch {
	case *isDistributed:
		opts = append(opts, distributed.Option())
	case *isDockerCluster:
		opts = append(opts, distributed.Option().SetMaster("master:45326"))
	}
	p.Run(opts...)

	log.Printf("processed %d rows successfully in %v\n", count, time.Since(start))
}

var count int64

func printRow(row *util.Row) error {
	fmt.Printf("\n\n%v\t", row.K[0])
	count++
	for _, v := range row.V {
		fmt.Printf("%v\t", v)
	}
	return nil
}

func flipKey(i int) gio.Mapper {
	return func(x []interface{}) error {
		row := append([]interface{}{x[i]}, x[:i]...)
		row = append(row, x[i+1:]...)
		return gio.Emit(row...)
	}
}

func readBlob(x []interface{}) error {
	repoPath := gio.ToString(x[1])
	blobHash := plumbing.NewHash(gio.ToString(x[5]))

	if blobHash.IsZero() {
		return gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], nil)
	}

	r, err := gogit.PlainOpen(repoPath)
	if err != nil {
		return errors.Wrapf(err, "could not open repo at %s", repoPath)
	}

	blob, err := r.BlobObject(blobHash)
	if err != nil {
		return errors.Wrapf(err, "could not retrieve blob object with hash %s", blobHash)
	}

	reader, err := blob.Reader()
	if err != nil {
		return errors.Wrapf(err, "could not read blob with hash %s", blobHash)
	}

	contents, err := ioutil.ReadAll(reader)
	reader.Close()
	if err != nil {
		return errors.Wrapf(err, "could not fully read blob with hash %s", blobHash)
	}

	return gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], contents)
}

func classifyLanguage(filenameIdx, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[filenameIdx])
		content := gio.ToBytes(x[contentIdx])
		lang := enry.GetLanguage(filename, content)
		return gio.Emit(append(x, lang)...)
	}
}
