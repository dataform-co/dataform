# Contributing

Dataform is a TypeScript project, and is fairly easy to build and run locally.
For help making changes, join our `#development` channel in [Slack](https://slack.dataform.co).

## Requirements

[Bazel](https://bazel.build) - Bazel is a build system, and this is the only dependency you need to build and run the entire project.

### Building

To check the project builds succesfully, run the following command:

```bash
bazel build ...
```
_Note: If you are running Bazel on a **Mac**, `bazel build...` may fail with a `Too many open files in system` error. This is [due to a limitation](https://github.com/bazelbuild/rules_closure/issues/218) on the default maximum open file descriptors. For OS Sierra you can increase this limit with this  command: `sudo sysctl -w kern.maxfiles=16384`._

### Test

To test the project, run the following command:

```bash
bazel test ...
```

_Note: Some integration tests may fail due to missing credentials. If you need to run these tests, reach out to the team on [Slack](https://slack.dataform.co)._

### Run the CLI

Print out the default help information:

```bash
bazel run cli:bin -- --help
```

Create a new `redshift` project in a temp directory:

```bash
bazel run cli:bin -- init redshift /tmp/test_project
```

See the [CLI documentation](https://docs.dataform.co/guides/command-line-interface/) for more commands.

## Run the documentation site

This will run the documentation site in development mode with live reload.

```bash
bazel run docs
```

You can view the documentation site at [localhost:3001](http://localhost:3001)
