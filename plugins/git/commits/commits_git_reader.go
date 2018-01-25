package commits

import (
	"strings"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type CommitsGitReader struct {
	repositoryID string
	commits      object.CommitIter
}

func New(r *git.Repository) *CommitsGitReader {

	remotes, _ := r.Remotes()
	commits, _ := r.CommitObjects()

	urls := remotes[0].Config().URLs
	repositoryID := strings.TrimPrefix(urls[0], "https://")

	return &CommitsGitReader{
		repositoryID: repositoryID,
		commits:      commits,
	}
}

func (r *CommitsGitReader) ReadHeader() (fieldNames []string, err error) {

	fieldNames = []string{
		"repositoryID",
		"commitHash",
		"treeHash",
		"parentHashes",
		"parentsCount",
		"message",
		"authorEmail",
		"authorName",
		"authorDate",
		"committerEmail",
		"committerName",
		"committerDate",
	}

	return fieldNames, nil
}

/*
root
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- index: integer (nullable = false)
 |-- hash: string (nullable = false)
 |-- message: string (nullable = false)
 |-- parents: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- parents_count: integer (nullable = false)
 |-- author_email: string (nullable = true)
 |-- author_name: string (nullable = true)
 |-- author_date: timestamp (nullable = true)
 |-- committer_email: string (nullable = true)
 |-- committer_name: string (nullable = true)
 |-- committer_date: timestamp (nullable = true)
*/

func (r *CommitsGitReader) Read() (row *util.Row, err error) {

	commit, err := r.commits.Next()
	if err != nil {
		return nil, err
	}

	commitHash := commit.Hash.String()
	message := commit.Message
	treeHash := commit.TreeHash.String()

	var parentHashes []string
	var parentsCount int
	for _, v := range commit.ParentHashes {
		parentHashes = append(parentHashes, v.String())
		parentsCount++
	}

	authorEmail := commit.Author.Email
	authorName := commit.Author.Name
	authorDate := commit.Author.When.Unix()
	committerEmail := commit.Committer.Email
	committerName := commit.Committer.Name
	committerDate := commit.Committer.When.Unix()

	return util.NewRow(util.Now(),
		r.repositoryID,
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
	), nil
}
