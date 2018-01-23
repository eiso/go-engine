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

	/*f := flow.New("Git pipeline").
	Read(git.Repositories("/home/mthek/engine/**", 1)).
	Printlnf("%s")
	*/
	f := flow.New("Git pipeline").
		Read(git.References("/home/mthek/engine/**", 1)).
		Pipe("grep", "grep remote").
		OutputRow(func(row *util.Row) error {
			fmt.Printf("%s : %s : %s\n ", gio.ToString(row.K[0]), gio.ToString(row.V[0]), gio.ToString(row.V[1]))
			return nil
		})

	if *isDistributed {
		f.Run(distributed.Option())
	} else if *isDockerCluster {
		f.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		f.Run()
	}
}
