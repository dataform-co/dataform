# Contributing

Dataform is a TypeScript project, and is fairly easy to build and run locally.
For help making changes, join our `#development` channel in [Slack](https://slack.dataform.co), or [email us](mailto:opensource@dataform.co) directly.

## Requirements

[Bazel](https://bazel.build) - Bazel is a build system, and this is the only dependency you need to build and run the entire project.

## Getting Started

First clone this repository, and navigate within.

### Run the CLI

Print out the default help information:

```bash
./scripts/run help
```

Create a new `redshift` project in a temp directory:

```bash
mkdir /tmp/test_project
./scripts/run init redshift /tmp/test_project
```

More commands can be found by substituting `dataform` with `./scripts/run` in the [CLI documentation](https://docs.dataform.co/guides/command-line-interface).

### Test

To test the project, run the following command:

```bash
bazel test -- tests/... -tests/integration/...
```

This runs all tests excluding the integration tests. If you need to run integration tests, please [get in touch](mailto:opensource@dataform.co) with the team.

_Note: A `java failed` error suggests that java needs to be installed._

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
