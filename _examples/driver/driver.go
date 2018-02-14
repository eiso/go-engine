package main

import (
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
	"github.com/pkg/errors"
)

func main() {
	var (
		query           = flag.String("query", "", "name the query you want to run")
		isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
		isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
		pathPtr         = flag.String("path", ".", "")
	)

	gio.Init()

	var path = *pathPtr

	log.Printf("analyzing %s", path)

	start := time.Now()

	if *query == "" {
		fmt.Print("please provide a query e.g. --query=test")
		os.Exit(0)
	}
	p, opts, err := queryExample(path, *query)
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
	opts    []flow.FlowOption
	regKey1 = gio.RegisterMapper(utils.ColumnToKey(1))
	regKey2 = gio.RegisterMapper(utils.ColumnToKey(1))
	regKey3 = gio.RegisterMapper(utils.ColumnToKey(3))

	one = gio.RegisterMapper(func(x []interface{}) error {
		return gio.Emit(1)
	})
	sum = gio.RegisterReducer(func(a, b interface{}) (interface{}, error) {
		return a.(int64) + b.(int64), nil
	})
)

func queryExample(path, query string) (*flow.Dataset, []flow.FlowOption, error) {
	f := flow.New(fmt.Sprintf("Driver: %s", query))
	var p *flow.Dataset

	switch query {
	case "test":
		p = f.Read(engine.Repositories(path, 1).
			References().
			Commits().
			Trees())
	default:
		return nil, nil, errors.New("this query is not implemented")
	}
	return p, opts, nil
}
