# Contributing

Dataform is a TypeScript project, and is fairly easy to build and run locally.
For help making changes, join our `#development` channel in [Slack](https://slack.dataform.co), or [email us](mailto:opensource@dataform.co) directly.

## Requirements

[Bazel](https://bazel.build) - Bazel is a build system, and this is the only dependency you need to build and run the entire project.

## Getting Started

First :fork_and_knife: [fork this repository](https://github.com/dataform-co/dataform/fork), clone it to your desktop, and navigate within.

### Run the CLI

Print out the default help information:

```bash
./scripts/run help
```

Create a new `redshift` toy project:

```bash
mkdir /tmp/test_project
./scripts/run init redshift /tmp/test_project
```

More commands can be found by substituting `dataform` with `./scripts/run` in the [CLI documentation](https://docs.dataform.co/guides/command-line-interface).

### Test

To test the Dataform project, run the following command:

```bash
bazel test -- tests/... -tests/integration/...
```

This runs all tests excluding the integration tests. If you need to run integration tests, please [get in touch](mailto:opensource@dataform.co) with the team.

_Note: A `java failed` error suggests that Java needs to be installed._

### Building

Running the CLI will build the required components. To build other components, for example the api, use:

```bash
bazel build api
```

_Note: Building the entire project with `Bazel build ...` will not work unless you have credentials provided by the team. A workaround to this is to create an empty `tools/stackdriver-github-bridge/env.yaml` file._

_Note: If you are running Bazel on a **Mac**, `bazel build ...` may fail with a `Too many open files in system` error. This is [due to a limitation](https://github.com/angular/angular-bazel-example/issues/178) on the default maximum open file descriptors. You can increase the limit by running `sudo sysctl -w kern.maxfiles=<LARGE_NUMBER>` (we use `65536`)._

### Run the documentation site

This will run the documentation site in development mode with live reload.

```bash
bazel run docs
```

You can view the documentation site at [localhost:3001](http://localhost:3001).

## Deciding what to contribute

A good place to start is by solving [Issues](https://github.com/dataform-co/dataform/issues), with the aim of understanding the code base well enough to create new features.

We also regularly run hackathons, but you'll have to [ask us directly](mailto:opensource@dataform.co) if you would like to participate.

Come meet us at [one of our events](https://www.google.com/search?q=dataform+events)!
