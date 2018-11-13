## Requirements

- Node (8+)
- NPM

## Setup

Install node dev packages:

```bash
npm i
```
Initialize lerna:

```bash
npx lerna init
```

## Build

Compile typescript for all packages:

```bash
./scripts/build/cli
```

## Run

Runs the command line app:

```bash
./scripts/run/cli --help
```

## Compile example project

Outputs a compiled JSON graph for one of the example projects:

```bash
./scripts/run/cli compile ts/example-bigquery
```

## Publish

Publish a new version with lerna:

```bash
npx lerna publish
```
