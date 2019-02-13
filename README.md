# Development

## Requirements

- [Bazel](https://bazel.build)

## Setup

Install node dev packages:

```bash
npm i
```

## Build

Build everything:

```bash
bazel build //...
```

## Run

Runs the command line app:

```bash
./scripts/run --help
```

## Compile example project

Setup the example projects
```
./scripts/
```

Outputs a compiled JSON graph for one of the example projects:

```bash
./scripts/run compile examples/bigquery
```

## Serve docs

```bash
npx next docs
```

## Run tests

Run mocha tests in `tests`:

```bash
./scripts/test
```
