# Contributing

Dataform is a TypeScript project, and is fairly easy to build and run locally.
For help making changes, join our `#development` channel in [Slack](https://slack.dataform.co), or [email us](mailto:team@dataform.co) directly.

Check out our [contribution guidelines](https://docs.dataform.co/community/contribution-guidelines) for more information.

## Requirements

[Bazel](https://bazel.build) - Bazel is a build system, and this is the only dependency you need to build and run the entire project.

## Getting Started

First :fork_and_knife: [fork this repository](https://github.com/dataform-co/dataform/fork), clone it to your desktop, and navigate within.

### Run the CLI

You can run the project as you would the `npm` installation of `@dataform/cli`, but replace `dataform` with `./scripts/run`.

For example, to print out the default help information:

```bash
./scripts/run help
```

Check the [docs](https://docs.dataform.co/guides/command-line-interface/) for more examples.

_Note: If you are running Bazel on a **Mac**, this or any step that requires building may fail with a `Too many open files in system` error. This is [due to a limitation](https://github.com/angular/angular-bazel-example/issues/178) on the default maximum open file descriptors. You can increase the limit by running `sudo sysctl -w kern.maxfiles=<LARGE_NUMBER>` (we use `65536`)._

### Test

To test the Dataform project, run the following command:

```bash
bazel test -- tests/... -tests/integration/...
```

This runs all tests excluding the integration tests. If you need to run integration tests, please [get in touch](mailto:opensource@dataform.co) with the team.

_Note: A `java failed` error suggests that Java needs to be installed._

### Building

Running the CLI will build the required components. To build all other components, use:

```bash
bazel build -- ... -tools/...
```

or if you're using `zsh`, then

```bash
bazel build -- "..." -tools/...
```

The projects folder here is not built as it requires an environment file, which can be provided from the team.

### Run the documentation site

This will run the documentation site in development mode with live reload.

```bash
bazel run docs
```

You can view the documentation site at [localhost:3001](http://localhost:3001).

## Deciding what to contribute

A good place to start is by solving [Issues](https://github.com/dataform-co/dataform/issues), with the aim of understanding the code base well enough to create new features.

We also regularly run hackathons, but you'll have to [ask us directly](mailto:team@dataform.co) if you would like to participate.

Come meet us at [one of our events](https://www.meetup.com/Data-First-London/)!
