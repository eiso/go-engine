package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
		query           = flag.String("query", "", "name the query you want to run")
		isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
		isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
	)

	gio.Init()

	var path = "."
	if args := flag.Args(); len(args) > 0 {
		path = args[0]
	}
	log.Printf("analyzing %s", path)

	start := time.Now()

	p, opts, err := queryExample(path, *query)
	if err != nil {
		fmt.Printf("could not load query: %s \n", err)
		os.Exit(0)
	}

	p.OutputRow(printRow)

	switch {
	case *isDistributed:
		opts = append(opts, distributed.Option())
	case *isDockerCluster:
		opts = append(opts, distributed.Option().SetMaster("master:45326"))
	}
	p.Run(opts...)

	log.Printf("\nprocessed %d rows successfully in %v\n", count, time.Since(start))
}

var (
	opts    []flow.FlowOption
	regKey1 = gio.RegisterMapper(columnToKey(1))
	refKey2 = gio.RegisterMapper(columnToKey(1))
	regKey3 = gio.RegisterMapper(columnToKey(3))
)

func queryExample(path, query string) (*flow.Dataset, []flow.FlowOption, error) {
	f := flow.New(fmt.Sprintf("Pipeline: %s", query))
	var p *flow.Dataset

	switch query {
	case "test":
		//TODO right now filter only works on referenceHash, hard coded, needs to abstract to key
		filters := make(map[int][]string)
		filters[2] = []string{"HEAD"}
		refOptions := git.Options{
			Filter:  filters,
			Reverse: true,
		}
		commitOptions := git.Options{}
		treeOptions := git.Options{}

		p = f.Read(git.Repositories(path, 1).
			References(refOptions).
			Commits(commitOptions).
			Trees(treeOptions))

	default:
		return nil, nil, errors.New("this query is not implemented")
	}
	return p, opts, nil
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

func columnToKey(i int) gio.Mapper {
	return func(x []interface{}) error {
		row := append([]interface{}{x[i]}, x[:i]...)
		row = append(row, x[i+1:]...)
		return gio.Emit(row...)
	}
}

func readBlob(repoPathIdx, blobHashIdx int) gio.Mapper {
	return func(x []interface{}) error {
		repoPath := gio.ToString(x[repoPathIdx])
		blobHash := plumbing.NewHash(gio.ToString(x[blobHashIdx]))

		if blobHash.IsZero() {
			return gio.Emit(x[:len(x)+1]...)
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

		return gio.Emit(append(x, contents)...)
	}
}

func classifyLanguage(filenameIdx, contentIdx int) gio.Mapper {
	return func(x []interface{}) error {
		filename := gio.ToString(x[filenameIdx])
		content := gio.ToBytes(x[contentIdx])
		lang := enry.GetLanguage(filename, content)
		return gio.Emit(append(x, lang)...)
	}
}
