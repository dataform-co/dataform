# Development

## Requirements

- Node (8+)
- NPM
- [Jekyll](https://jekyllrb.com/docs/installation/)

## Setup

Install node dev packages:

```bash
npm i
```

## Build

Compile typescript for all packages:

```bash
./scripts/build
```

## Run

Runs the command line app:

```bash
./scripts/run --help
```

## Compile example project

Outputs a compiled JSON graph for one of the example projects:

```bash
./scripts/run compile examples/bigquery
```

## Serve docs

```bash
./scripts/docs/serve
```

## Run tests

Run mocha tests in `tests`:

```bash
./scripts/test
```

## Publish

Publish a new version with lerna:

```bash
npx lerna publish
```
