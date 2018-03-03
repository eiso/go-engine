package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"

	engine "github.com/eiso/go-engine"
	"github.com/eiso/go-engine/utils"

	"net/http"
	_ "net/http/pprof"
)

func main() {

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:8080", nil))
	}()

	var (
		query           = flag.String("query", "", "name the query you want to run")
		isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
		isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
		pathPtr         = flag.String("path", ".", "")
		partitions      = flag.Int("partitions", 1, "number of partitions")
	)

	gio.Init()

	if *query == "" {
		fmt.Print("please provide a query e.g. --query=test")
		os.Exit(0)
	}

	path := *pathPtr
	if path == "." {
		log.Print("analyzing the current directory, provide --path=/your/repos for a different path")
	} else {
		log.Printf("analyzing %s", path)
	}

	start := time.Now()

	p, opts, err := queryExample(path, *query, *partitions)
	if err != nil {
		fmt.Printf("could not load query: %s \n", err)
		os.Exit(0)
	}

	p.OutputRow(utils.PrintRow)

	switch {
	case *isDistributed:
		opts = append(opts, distributed.Option())
	case *isDockerCluster:
		opts = append(opts, distributed.Option().SetMaster("master:45326"))
	}
	p.Run(opts...)

	log.Printf("\nprocessed rows successfully in %v\n", time.Since(start))
}

var (
	opts []flow.FlowOption
)

func queryExample(path, query string, partitions int) (*flow.Dataset, []flow.FlowOption, error) {
	f := flow.New(fmt.Sprintf("Driver: %s", query))
	var p *flow.Dataset

	switch query {
	case "test":
		p = f.Read(engine.Repositories(path, partitions).
			References())
	default:
		return nil, nil, errors.New("this query is not implemented")
	}
	return p, opts, nil
}
