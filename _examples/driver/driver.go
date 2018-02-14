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
	"github.com/chrislusf/gleam/util"
	engine "github.com/eiso/go-engine"
	"github.com/eiso/go-engine/utils"
	"github.com/pkg/errors"
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

	if *query == "" {
		fmt.Print("please provide a query e.g. --query=test")
		os.Exit(0)
	}
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
	regKey1 = gio.RegisterMapper(utils.ColumnToKey(1))
	refKey2 = gio.RegisterMapper(utils.ColumnToKey(1))
	regKey3 = gio.RegisterMapper(utils.ColumnToKey(3))
)

func queryExample(path, query string) (*flow.Dataset, []flow.FlowOption, error) {
	f := flow.New(fmt.Sprintf("Driver: %s", query))
	var p *flow.Dataset

	switch query {
	case "test":
		//TODO right now filter only works on referenceHash, hard coded, needs to abstract to key
		filter := func(opts *engine.Options) {
			filters := make(map[int][]string)
			filters[2] = []string{"HEAD", "refs/heads/develop"}
			opts.Filter = filters
		}

		p = f.Read(engine.Repositories(path, 1).
			References(filter).
			//Commits().
			Trees())
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
