package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/util"

	"gopkg.in/bblfsh/client-go.v2"
	protocol "gopkg.in/bblfsh/sdk.v1/protocol"
	enry "gopkg.in/src-d/enry.v1"
	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

var (
	isDistributed   = flag.Bool("distributed", false, "run in distributed or not")
	isDockerCluster = flag.Bool("onDocker", false, "run in docker cluster")

	registeredRefsKeyForCommits  = gio.RegisterMapper(refs{}.keyForCommits)
	registeredCommitsKeyForRefs  = gio.RegisterMapper(commits{}.keyForRefs)
	registeredCommitsKeyForTrees = gio.RegisterMapper(commits{}.keyForTrees)
	registeredTreesKeyForCommits = gio.RegisterMapper(trees{}.keyForCommits)
	registeredUAST               = gio.RegisterMapper(uast)
	registeredReadBlob           = gio.RegisterMapper(readBlob)
	registeredClassifyLanguages  = gio.RegisterMapper(classifyLanguages)
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("Git pipeline")

	path := "/home/mthek/engine/keras"

	//repos := f.Read(git.Repositories(path, 1))

	/*
		refs := f.Read(git.References(path, 1)).
			Map("RefsJoinCommits", registeredRefsKeyForCommits)
		commits := f.Read(git.Commits(path, 1)).
			Map("CommitsJoinRefs", registeredCommitsKeyForRefs)

		join := commits.LeftOuterJoinByKey("Commits & Refs", refs)


		commits := f.Read(git.Commits(path, 1)).
			Map("CommitsJoinRefs", registeredCommitsKeyForTrees)
		trees := f.Read(git.Commits(path, 1)).
			Map("CommitsJoinTrees", registeredTreesKeyForCommits)

		join := trees.LeftOuterJoinByKey("Trees & Commits", commits)
	*/

	//blobs := f.Read(git.Blobs(path, 1))

	q := join.OutputRow(func(row *util.Row) error {
		fmt.Printf("\n %s : %s : %s\n",
			gio.ToString(row.K[0]),
			gio.ToString(row.V[0]),
			gio.ToString(row.V[11]),
		)
		return nil
	})

	//Map("readBlob", registeredReadBlob).
	//Map("readBlob", registeredClassifyLanguages)

	if *isDistributed {
		q.Run(distributed.Option())
	} else if *isDockerCluster {
		q.Run(distributed.Option().SetMaster("master:45326"))
	} else {
		q.Run()
	}
}

type refs struct{}

func (refs) keyForCommits(x []interface{}) error {
	repositoryID := x[0]
	refHash := x[1]
	refName := x[2]

	gio.Emit(
		refHash,
		refName,
		repositoryID,
	)
	return nil
}

type commits struct{}

func (commits) keyForRefs(x []interface{}) error {
	repositoryID := x[0]
	commitHash := x[1]
	treeHash := x[2]
	parentHashes := x[3]
	parentsCount := x[4]
	message := x[5]
	authorEmail := x[6]
	authorName := x[7]
	authorDate := x[8]
	committerEmail := x[9]
	committerName := x[10]
	committerDate := x[11]

	gio.Emit(
		commitHash,
		treeHash,
		parentHashes,
		parentsCount,
		message,
		authorEmail,
		authorName,
		authorDate,
		committerEmail,
		committerName,
		committerDate,
		repositoryID,
	)
	return nil
}

func (commits) keyForTrees(x []interface{}) error {
	repositoryID := x[0]
	commitHash := x[1]
	treeHash := x[2]
	parentHashes := x[3]
	parentsCount := x[4]
	message := x[5]
	authorEmail := x[6]
	authorName := x[7]
	authorDate := x[8]
	committerEmail := x[9]
	committerName := x[10]
	committerDate := x[11]

	gio.Emit(
		treeHash,
		commitHash,
		parentHashes,
		parentsCount,
		message,
		authorEmail,
		authorName,
		authorDate,
		committerEmail,
		committerName,
		committerDate,
		repositoryID,
	)
	return nil
}

type trees struct{}

func (trees) keyForCommits(x []interface{}) error {
	repositoryID := x[0]
	treeHash := x[1]

	gio.Emit(
		treeHash,
		repositoryID,
	)
	return nil
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

func readBlob(x []interface{}) error {
	repoPath := gio.ToString(x[1])
	blobHash := plumbing.NewHash(gio.ToString(x[5]))
	contents := []byte("")

	if !blobHash.IsZero() {
		r, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return err
		}

		blob, err := r.BlobObject(blobHash)
		if err != nil {
			return err
		}

		reader, err := blob.Reader()
		if err != nil {
			return err
		}

		contents, err = ioutil.ReadAll(reader)
		if err != nil {
			return err
		}
	}

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], contents)

	return nil
}

// TODO finish once tree entries are implemented
// Requires file path to work
func classifyLanguages(x []interface{}) error {
	contents := gio.ToBytes(x[6])

	lang := enry.GetLanguage("random.xxx", contents)

	gio.Emit(x[0], x[1], x[2], x[3], x[4], x[5], x[6], lang)
	return nil
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
