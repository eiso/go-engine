package main

import (
	"flag"
	"fmt"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"
	"gopkg.in/bblfsh/client-go.v2"
	protocol "gopkg.in/bblfsh/sdk.v1/protocol"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")

	registeredUAST = gio.RegisterMapper(uast)
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("Git pipeline")

	path := "/home/mthek/engine-small/**"

	repos := f.Read(git.Repositories(path, 1))

	refs := f.Read(git.References(path, 1)).
		Pipe("grep", "grep refs")

	blobs := f.Read(git.Blobs(path, 1))

	join1 := repos.JoinByKey("Repos & Refs", refs)

	join := join1.JoinByKey("Repos & Refs & Blobs", blobs)

	q := join.Map("uast", registeredUAST).
		OutputRow(func(row *util.Row) error {
			repositoryID := gio.ToString(row.K[0])
			repositoryURLs := row.V[0]
			refHash := gio.ToString(row.V[1])
			refName := gio.ToString(row.V[2])
			blobHash := gio.ToString(row.V[3])
			blobContent := truncateString(gio.ToString(row.V[4]), 20)
			language := gio.ToString(row.V[5])
			fmt.Printf("%s : %s : %s : %s : %s : %s : %s\n",
				repositoryID,
				repositoryURLs,
				refHash,
				refName,
				blobHash,
				blobContent,
				language)
			return nil
		})

	if *isDistributed {
		q.Run(distributed.Option())
	} else if *isDockerCluster {
		q.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		q.Run()
	}
}

func truncateString(str string, num int) string {
	b := str
	if len(str) > num {
		if num > 3 {
			num -= 3
		}
		b = str[0:num] + "..."
	}
	return b
}

func uast(x []interface{}) error {

	client, err := bblfsh.NewClient("0.0.0.0:9432")
	if err != nil {
		panic(err)
	}

	blob := gio.ToString(x[4])

	// TODO language classification with Enry as an earlier step
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
