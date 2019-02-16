# Development

## Requirements

- [Bazel](https://bazel.build)

## Build

```bash
bazel build //...
```

## Run

```bash
./scripts/run --help
```

## Create and compile example project

```bash
./scripts/run init /tmp/dataform-project
./scripts/run compile /tmp/dataform-project
```

## Setup docs site

```bash
npm i --prod
```

## Serve docs site

```bash
./scripts/docs/serve
```

## Run tests

```bash
./scripts/test
```
