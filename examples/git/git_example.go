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

	repos := f.Read(git.Repositories("/home/mthek/engine/**", 1))

	refs := f.Read(git.References("/home/mthek/engine/**", 1)).
		Pipe("grep", "grep remote")

	join := repos.Join("Repository Data", refs, flow.OrderBy(1, true)).
		OutputRow(func(row *util.Row) error {
			fmt.Printf("%s : %s : %s : %s\n ", gio.ToString(row.K[0]), row.V[0], gio.ToString(row.V[1]), gio.ToString(row.V[2]))
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
