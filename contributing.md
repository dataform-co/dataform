# Contributing

Dataform is a TypeScript project, and is fairly easy to build and run locally.
For help making changes, join our `#development` channel in [Slack](https://slack.dataform.co), or [email us](mailto:team@dataform.co) directly.

Dataform operates similar to most open source projects on GitHub. If you've never done this before, here's [a nice project to get you up to speed](https://github.com/firstcontributions/first-contributions).

## Getting Started

First :fork_and_knife: [fork this repository](https://github.com/dataform-co/dataform/fork), clone it to your desktop, and navigate within.

### Requirements

[Bazel](https://bazel.build) - Bazel is a build system, and this is the only dependency you need to build and run the entire project.

### Run the CLI

You can run the project as you would the `npm` installation of `@dataform/cli`, but replace `dataform` with `./scripts/run`.

For example, to print out the default help information:

```bash
./scripts/run help
```

Check the [docs](https://docs.dataform.co/dataform-cli) for more examples.

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

## The Contribution Process

1. Decide on what you'd like to contribute. The majority of open-source contributions come from:

   1. Someone deciding they want a feature that is not currently present, and which isn't a priority for the team.

   1. Embracing the community aspect of open source (or getting that commit count up) and solving an issue.

1. Plan out the change, and whether it is feasible.

   1. If you're unsure of the scope of the change, then message us on [Slack](https://slack.dataform.co).

   1. We'd much prefer multiple smaller code changes than a single large one.

   1. Avoid changing core functionality over a long time frame. Our development process is very dynamic, so if your code depends on lots of other parts of the project, then it is likely to be out of date by the time you finish!

   1. If you're solving an issue, be sure to comment to make it known that you are currently solving it. Unless we have worked with you before, it is unlikely that we will lock the issue to you.

1. Begin materialising your masterpiece.

1. Once done, merge from master, review your code, run the tests, **[check for common mistakes](#common-pull-request-mistakes)** and then open a pull request.

   1. Tidy the code by removing erronous log statements. Comment difficult to interpret sections. Make sure functions are names appropriately. We will review the pull request mainly by the git difference.

   1. Assign a reviewer. Pick anyone on the team who seems to contribute a lot and they will refer it onto whoever is most responsible for the given subsystem.

1. Discuss and process any changes requested.

   1. It's unlikely your pull request will be perfect immediately; there will likely be some changes requested, whether it's to do with style or a more fundamental issue.

   1. The automated integration tests must pass.

   1. Once a pull request is accepted and all automated integration tests are passing, we will merge it for you.

### Reporting Issues (!)

Another way we'd love for you to contribute is by flagging any issues you find. First check through the list of [existing issues](https://github.com/dataform-co/dataform/issues) for anything similar, in order to avoid duplicates. If not, then full steam ahead!

### Adding Tutorials

We really value the addition of [tutorials](../getting_started/examples). Documentation here is written in markdown which is then compiled to a website. You can find the raw markdown files [here](https://github.com/dataform-co/dataform/tree/master/content/docs).

### Promoting Dataform

If you're using Dataform for interesting projects then please let people know! Reach out to [team@dataform.co](team@dataform.co) for marketing support.

### Common Pull Request Mistakes

1. Is it too long? Small pull requests are easier to review and merge. If you are planning on making a larger change, then talk to the team and write a document on the design.

1. Have you appropriately increased test coverage? If the operation of the change is not already tested, then tests will need to be written.

1. Is it a hack? Does it solve the problem, but it is not reliable, reproducable or extendable? In other words, [does it smell](https://en.wikipedia.org/wiki/Code_smell)?

1. Have you changed whitespace or touched unrelated code? Please avoid this as it makes pull requests far more difficult to review.

1. Are the comments useful, and is the code readable? Are the function and variable names appropriate?
