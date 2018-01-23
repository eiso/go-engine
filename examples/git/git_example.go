package main

import (
	"flag"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("Git pipeline").
		//	Read(git.References("/home/mthek/engine/**", 1)).
		Read(git.Repositories("/home/mthek/engine/**", 1)).
		Printlnf("%s")

	if *isDistributed {
		f.Run(distributed.Option())
	} else if *isDockerCluster {
		f.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		f.Run()
	}
}
