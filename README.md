go-engine
-------------------

**WARNING: This library is a WIP and not production ready.**

This library is currently a learning project that implements the [source{d} engine](https://github.com/src-d/engine) in Go using [Gleam](https://github.com/chrislusf/gleam/) (Go implementation of MapReduce). 

The heavy lifting of working with Git repositories is done by [go-git](https://github.com/src-d/go-git).

**This projects implements the following Git data sources:**

- repositories
- references
- commits
- trees
- blobs (WIP)

### UDF's TO-DO
- [x] `readBlob` read the content of a blob based on its hash
- [x] `classifyLanguage` implements [enry](https://github.com/src-d/enry) to classify the programming language of the blobs content 
- [ ] `extractUAST` parses a blob using [Babelfish](https://doc.bblf.sh/)  

### TO-DO
- [ ] Research how to add named columns to Gleam
  - Discussed it with Chris Lu, author of Gleam: 

    > _Currently the header names are not used because they are not carried over from one step to the next, and header names can not be determined after one map step. So there would need some new ideas to re-engineer this._
  - @campoy has an interesting idea on how to use this inspired by protobuf
- [ ] The `trees` GitDataSource is missing the hashes for folders (tree object hashes), currently it only emits rows for files
  - Not sure yet if this is a feature or a bug
- [ ] Implement the [queries from QuerySetApp](https://github.com/mcarmonaa/QuerySetApp/blob/master/src/main/scala/tech/sourced/queryset/SourcedQueries.scala#L26)
- [ ] Research on adding bitmap reader to go-git:
  - https://kscherer.github.io/git/2015/05/15/git-and-bitmaps
  - https://githubengineering.com/counting-objects/
- [ ] Modify the blobs data source to implement chained iterators
- [ ] Generalize the filter function so that it can select which column to operate on
- [ ] Refactor `source/` to be a flat file structure of git sources

### Data Model

This does not follow the variable names in the code, it's purely to understand the git data sources' model.

#### repositories
* repositoryPath `string`
* headHash `string`
  * the hash of the reference that HEAD is pointing too
* remoteURLs `[string]`

#### references
* repositoryPath `string`
* refHash `string`
* refName `string`
* commitHash `string`
  * this is not only the commit that equals to refHash, but also every parent commit in that branch
* refIsRemote `bool`

#### commits
* repositoryPath `string`
* commitHash `string`
* treeHash `string`
* parentHashes `[string]`
* parentsCount `int`
* message `string`
* authorEmail `string`
* authorName `string`
* authorDate `string`
* committerEmail `string`
* comitterName `string`
* committerDate `string`

#### trees
* repositoryPath `string`
* refHash `string`
* treeHashFromCommit `string`
* blobHash `string`
* filePath `string`
* blobSize `int`
* isBinary `bool`

Slide: [Understanding Git Internals](https://www.slideshare.net/JeffKunkle/understanding-git#48)