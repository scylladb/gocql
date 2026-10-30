# Contributing to the ScyllaDB GoCQL Driver

**TL;DR** - this manifesto sets out the bare minimum requirements for submitting a patch to gocql.

This guide outlines the process of landing patches in gocql and the general approach to maintaining the code base.

## Background

The goal of the gocql project is to provide a stable and robust CQL driver for Go. This is a community driven project that is coordinated by a small team of core developers.

## Minimum Requirement Checklist

The following is a check list of requirements that need to be satisfied in order for us to merge your patch:

* You should raise a pull request to scylladb/gocql on Github
* The pull request has a title that clearly summarizes the purpose of the patch
* The motivation behind the patch is clearly defined in the pull request summary
* You agree that your contribution is donated to the Apache Software Foundation (appropriate copyright is on all new files)
* The patch will merge cleanly
* The test coverage does not fall
* The merge commit passes the regression test suite on GitHub Actions
* `go fmt` has been applied to the submitted code
* A correctly formatted commit message, see below

If there are any requirements that can't be reasonably satisfied, please state this either on the pull request or as part of discussion on the mailing list. Where appropriate, the core team may apply discretion and make an exception to these requirements.

## Commit Message

The commit message format should be:

```
<short description>

<reason why the change is needed>

Patch by <authors>; reviewed by <Reviewers> for #####
```

Short description should:
* Be a short sentence.
* Start with a capital letter.
* Be written in the present tense.
* Summarize what is changed, not why it is changed.

Short description should not:
* End with a period.
* Use the word Fixes . Most commits fix something.

Long description / Reason:
* Should describe why the change is needed. What is fixed by the change? Why it it was broken before? What use case does the new feature solve?
* Consider adding details of other options that you considered when implementing the change and why you made the design decisions you made.

## Beyond The Checklist

In addition to stating the hard requirements, there are a bunch of things that we consider when assessing changes to the library. These soft requirements are helpful pointers of how to get a patch landed quicker and with less fuss.

### General QA Approach

The Scylla project needs to consider the ongoing maintainability of the library at all times. Patches that look like they will introduce maintenance issues for the team will not be accepted.

Your patch will get merged quicker if you have decent test cases that provide test coverage for the new behavior you wish to introduce.

Unit tests are good, integration tests are even better. An example of a unit test is `marshal_test.go` - this tests the serialization code in isolation. `cassandra_test.go` is an integration test suite that is executed against every version of Cassandra that gocql supports as part of the CI process on Travis.

That said, the point of writing tests is to provide a safety net to catch regressions, so there is no need to go overboard with tests. Remember that the more tests you write, the more code we will have to maintain. So there's a balance to strike there.

#### Typed API and module checks

The repository has one module, `github.com/scylladb/gocql/v2`, including codec,
compression and replay benchmark packages. Run `make check-go-mod-drift`,
`go test -race -tags unit -timeout=5m ./...` and compilation with
`-tags 'integration gocql_debug'`. New examples must compile with the normal
consumer package build and use typed codecs.

Use the owned native fixture runner described in tests/fixtures/README.md for
actual database tests. Select Cassandra protocol 5 explicitly for v5 segments;
Scylla fixture protocol rejection is not a v5 pass. Replay regeneration and
operation checks are documented in tests/bench/README.md. Comprehensive native
acceptance and performance remain separate source-bound gates in plan.md.
