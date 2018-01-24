package trees

import (
	"strings"

	"github.com/chrislusf/gleam/util"
	git "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing/object"
)

type TreesGitReader struct {
	repositoryID string
	trees        *object.TreeIter
}

func New(r *git.Repository) *TreesGitReader {

	remotes, _ := r.Remotes()
	tree, _ := r.TreeObjects()

	urls := remotes[0].Config().URLs
	repositoryID := strings.TrimPrefix(urls[0], "https://")

	return &TreesGitReader{
		repositoryID: repositoryID,
		trees:        tree,
	}
}

func (r *TreesGitReader) ReadHeader() (fieldNames []string, err error) {
	return nil, nil
}

/*
root
 |-- commit_hash: string (nullable = false)
 |-- repository_id: string (nullable = false)
 |-- reference_name: string (nullable = false)
 |-- path: string (nullable = false)
 |-- blob: string (nullable = false)
*/

func (r *TreesGitReader) Read() (row *util.Row, err error) {

	tree, err := r.trees.Next()
	if err != nil {
		return nil, err
	}
	/*
		tree.Files().ForEach(func(file *object.File) error {
			log.Printf("Hash: %s \t Entries: %s \t Filename: %s", tree.Hash.String(), tree.Entries, file.Name)
			//row.WriteTo(os.Stdout)
			return nil
		})
	*/
	return util.NewRow(util.Now(), r.repositoryID, tree.Hash.String()), nil
}
