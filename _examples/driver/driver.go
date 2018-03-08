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
	enry "gopkg.in/src-d/enry.v1"

	engine "github.com/eiso/go-engine"
	"github.com/eiso/go-engine/utils"

	"net/http"
	_ "net/http/pprof"
)

func main() {

	var (
		query           = flag.String("query", "", "name the query you want to run")
		isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
		isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")
		pathPtr         = flag.String("path", ".", "")
		partitions      = flag.Int("partitions", 1, "number of partitions")
	)

	go func() {
		log.Println(http.ListenAndServe("0.0.0.0:8080", nil))
	}()

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

var getLanguagesFromBlobs = gio.RegisterMapper(utils.NamedMapper(
	[]string{"lang"},
	func(row []interface{}, getByName utils.GetByNameFunc) error {
		isBinary := getByName(row, "isBinary").(bool)
		if isBinary {
			return nil
		}

		path := getByName(row, "path").(string)
		content := gio.ToBytes(getByName(row, "content"))
		lang := enry.GetLanguage(path, content)
		if lang == "" {
			return nil
		}

		return gio.Emit(lang)
	}))

var countGroups = gio.RegisterMapper(func(x []interface{}) error {
	key := x[0]
	count := len(x) - 1
	return gio.Emit(key, count)
})

func queryExample(path, query string, partitions int) (*flow.Dataset, []flow.FlowOption, error) {
	f := flow.New(fmt.Sprintf("Driver: %s on %s", query, path))
	var p *flow.Dataset

	switch query {
	case "mostUsedLanguages":
		numberOfLangs := 10
		fmt.Printf(">>> %d most used languages:\n", numberOfLangs)
    
    		p = f.Read(engine.Repositories(path, partitions).
			References().
			Commits().
			Trees().	
      			Blobs().
			WithHeaders()).
			Map("classify languages", getLanguagesFromBlobs).
			GroupBy("group by lang", flow.Field(1)).
			Map("group count", countGroups).
			Top("top", numberOfLangs, flow.OrderBy(2, false))
		
	case "repositories":
		p = f.Read(engine.Repositories(path, partitions))
	case "references":
		p = f.Read(engine.Repositories(path, partitions).
			References())
	case "referencesMaster":
		p = f.Read(engine.Repositories(path, partitions).
			References().Filter("refs/heads/master"))
	case "commits":
		p = f.Read(engine.Repositories(path, partitions).
			References().
			Commits())
	case "commitsMaster":
		p = f.Read(engine.Repositories(path, partitions).
			References().Filter("refs/heads/master").
			Commits())
	case "trees":
		p = f.Read(engine.Repositories(path, partitions).
			References().
			Commits().
			Trees())
	case "blobs":
		p = f.Read(engine.Repositories(path, partitions).
			References().
			Commits().
			Trees().
			Blobs())
	default:
		return nil, nil, errors.New("this query is not implemented")
	}
	return p, opts, nil
}
