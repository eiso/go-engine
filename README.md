go-engine
-------------------

**WARNING: This library is a WIP and is not production ready.**

This library is currently a learning project that implements the [source{d} engine](https://github.com/src-d/engine) in Go using [Gleam](https://github.com/chrislusf/gleam/) (Go implementation of MapReduce). 

The heavy lifting of working with Git repositories is done by [go-git](https://github.com/src-d/go-git).

### To-do
- [ ] Implement the [queries from QuerySetApp](https://github.com/mcarmonaa/QuerySetApp/blob/master/src/main/scala/tech/sourced/queryset/SourcedQueries.scala#L26)
- [ ] Generalize the filter function
- [ ] Improve the siva reading to turn rooted repositories into individual ones
- [ ] Add a Babelfish deployment to k8s
- [ ] UDF's:
  - [x] `readBlob` read the content of a blob based on its hash
  - [x] `classifyLanguage` implements [enry](https://github.com/src-d/enry) to classify the programming language of the blobs content 
  - [ ] `extractUAST` parses a blob using [Babelfish](https://doc.bblf.sh/)  

### Future ideas:

- [ ] Research how to add named columns to Gleam
- [ ] Update gleam to use both inner & outer IP's so that binaries can be sent to agents from any IP
- [ ] Research on adding bitmap reader to go-git:
  - https://kscherer.github.io/git/2015/05/15/git-and-bitmaps
  - https://githubengineering.com/counting-objects/