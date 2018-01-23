package main

import (
	"flag"
	"fmt"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
	"github.com/chrislusf/gleam/util"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("Git pipeline")

	path := "/home/mthek/engine/**"

	repos := f.Read(git.Repositories(path, 1))

	refs := f.Read(git.References(path, 1)).
		Pipe("grep", "grep refs")

	blobs := f.Read(git.Blobs(path, 1))

	join1 := repos.JoinByKey("Repos & Refs", refs)

	join := join1.JoinByKey("Repos & Refs & Blobs", blobs).
		OutputRow(func(row *util.Row) error {
			repositoryID := gio.ToString(row.K[0])
			repositoryURLs := row.V[0]
			refHash := gio.ToString(row.V[1])
			refName := gio.ToString(row.V[2])
			blobHash := gio.ToString(row.V[3])
			blobContent := truncateString(gio.ToString(row.V[4]), 20)
			fmt.Printf("%s : %s : %s : %s : %s : %s\n",
				repositoryID,
				repositoryURLs,
				refHash,
				refName,
				blobHash,
				blobContent)
			return nil
		})

	if *isDistributed {
		join.Run(distributed.Option())
	} else if *isDockerCluster {
		join.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		join.Run()
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
