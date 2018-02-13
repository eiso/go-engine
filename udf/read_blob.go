package udf

import (
	"io/ioutil"

	"github.com/chrislusf/gleam/gio"
	"github.com/pkg/errors"

	gogit "gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/plumbing"
)

// ReadBlob TODO
// Needs to be refactored to accept the blob itself, not the hash
func ReadBlob(repoPathIdx, blobHashIdx int) gio.Mapper {
	return func(x []interface{}) error {
		repoPath := gio.ToString(x[repoPathIdx])
		blobHash := plumbing.NewHash(gio.ToString(x[blobHashIdx]))

		if blobHash.IsZero() {
			return gio.Emit(x[:len(x)+1]...)
		}

		r, err := gogit.PlainOpen(repoPath)
		if err != nil {
			return errors.Wrapf(err, "could not open repo at %s", repoPath)
		}

		blob, err := r.BlobObject(blobHash)
		if err != nil {
			return errors.Wrapf(err, "could not retrieve blob object with hash %s", blobHash)
		}

		reader, err := blob.Reader()
		if err != nil {
			return errors.Wrapf(err, "could not read blob with hash %s", blobHash)
		}

		contents, err := ioutil.ReadAll(reader)
		reader.Close()
		if err != nil {
			return errors.Wrapf(err, "could not fully read blob with hash %s", blobHash)
		}

		return gio.Emit(append(x, contents)...)
	}
}
