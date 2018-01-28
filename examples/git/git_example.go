package main

import (
	"flag"
	"fmt"
	"io/ioutil"

	"github.com/chrislusf/gleam/distributed"
	"github.com/chrislusf/gleam/flow"
	"github.com/chrislusf/gleam/gio"
	"github.com/chrislusf/gleam/plugins/git"
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

	registeredCommitsJoinRefsKeyForTrees    = gio.RegisterMapper(commitsJoinRefs{}.keyForTrees)
	registeredTreesJoinCommitsKeyForCommits = gio.RegisterMapper(treesJoinCommits{}.keyForCommits)

	registeredUAST              = gio.RegisterMapper(uast)
	registeredReadBlob          = gio.RegisterMapper(readBlob)
	registeredClassifyLanguages = gio.RegisterMapper(classifyLanguages)
)

func main() {

	flag.Parse() // optional, since gio.Init() will call this also.
	gio.Init()   // If the command line invokes the mapper or reducer, execute it and exit.

	f := flow.New("Git pipeline")

	path := "/home/mthek/projects/enginerepos/**"

	//	repos := f.Read(git.Repositories(path, 1))

	// KEY: refHash
	refs := f.Read(git.References(path, 1)).
		Map("RefsJoinCommits", registeredRefsKeyForCommits)
	// KEY: commitHash (== refHash)
	commits := f.Read(git.Commits(path, 1)).
		Map("CommitsJoinRefs", registeredCommitsKeyForRefs)

	// KEY: commitHash
	commitsJoinRefs := commits.LeftOuterJoinByKey("Commits & Refs", refs).
		Map("CommitsJoinRefsKeyTrees", registeredCommitsJoinRefsKeyForTrees)

	// KEY: treeHash
	/*commits2 := f.Read(git.Commits(path, 1)).
		Map("CommitsJoinRefs", registeredCommitsKeyForTrees)
	// KEY: treeHash
	trees := f.Read(git.Trees(path, 1)).
		Map("CommitsJoinTrees", registeredTreesKeyForCommits)

	// KEY: commitHash
	treesJoinCommits := trees.LeftOuterJoinByKey("Trees & Commits", commits2).
		Map("TreesJoinCommitsKeyCommits", registeredTreesJoinCommitsKeyForCommits)

	commitsJoinTreesJoinRefs := treesJoinCommits.LeftOuterJoinByKey("Refs & Commits & Trees", commitsJoinRefs)
	*/
	q := commitsJoinRefs.OutputRow(func(row *util.Row) error {

		fmt.Printf("\n%s : %s : %s : %s\n",
			gio.ToString(row.K[0]),
			gio.ToString(row.V[0]),
			gio.ToString(row.V[1]),
			gio.ToString(row.V[2]),
			gio.ToString(row.V[3]),
			gio.ToString(row.V[4]),
			gio.ToString(row.V[5]),
			gio.ToString(row.V[6]),
			gio.ToString(row.V[7]),
			gio.ToString(row.V[8]),
			gio.ToString(row.V[9]),
			gio.ToString(row.V[10]),
		/*		gio.ToString(row.V[11]),
				gio.ToString(row.V[12]),
				gio.ToString(row.V[13]),
				gio.ToString(row.V[14]),
				gio.ToString(row.V[15]),
				gio.ToString(row.V[16]),
				gio.ToString(row.V[17]),
				gio.ToString(row.V[18]),
				gio.ToString(row.V[19]),
				gio.ToString(row.V[20]),
				gio.ToString(row.V[21]),
				gio.ToString(row.V[22]),
				gio.ToString(row.V[23]),
				gio.ToString(row.V[24]),*/
		)
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

type refs struct{}

func (refs) keyForCommits(x []interface{}) error {
	// repositoryID := x[0]
	commitHash := x[3]
	refHash := x[1]
	refName := x[2]

	gio.Emit(
		commitHash,
		refHash,
		refName,
	)
	return nil
}

type commits struct{}

func (commits) keyForRefs(x []interface{}) error {
	// repositoryID := x[0]
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
	)
	return nil
}

func (commits) keyForTrees(x []interface{}) error {
	//	repositoryID := x[0]
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
	)
	return nil
}

type trees struct{}

func (trees) keyForCommits(x []interface{}) error {
	//repositoryID := x[0]
	treeHash := x[1]
	fileName := x[2]
	blobHash := x[3]
	blobSize := x[4]
	isBinary := x[5]

	gio.Emit(
		treeHash,
		fileName,
		blobHash,
		blobSize,
		isBinary,
	)
	return nil
}

//--------JOINS--------//

type commitsJoinRefs struct{}

func (commitsJoinRefs) keyForTrees(x []interface{}) error {

	// -- commits
	commitHash := x[0]
	treeHash := x[1]
	parentHashes := x[2]
	parentsCount := x[3]
	message := x[4]
	authorEmail := x[5]
	authorName := x[6]
	authorDate := x[7]
	committerEmail := x[8]
	committerName := x[9]
	committerDate := x[10]
	// -- refs
	refName := x[11]

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
		refName,
	)
	return nil
}

type treesJoinCommits struct{}

func (treesJoinCommits) keyForCommits(x []interface{}) error {

	// -- trees
	treeHash := x[0]
	fileName := x[1]
	blobHash := x[2]
	blobSize := x[3]
	isBinary := x[4]
	// -- commits
	commitHash := x[5]
	parentHashes := x[6]
	parentsCount := x[7]
	message := x[8]
	authorEmail := x[9]
	authorName := x[10]
	authorDate := x[11]
	committerEmail := x[12]
	committerName := x[13]
	committerDate := x[14]

	gio.Emit(
		commitHash,
		treeHash,
		fileName,
		blobHash,
		blobSize,
		isBinary,
		parentHashes,
		parentsCount,
		message,
		authorEmail,
		authorName,
		authorDate,
		committerEmail,
		committerName,
		committerDate,
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
