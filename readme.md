# Python Dataform

This is an experimental Dataform version that uses Python as the templating language.

Please don't use or depend on this!

## Quickstart

### Prerequesites

- `python3.10` - you probably already have this.
- `protoc` - you likely already have this. If not, [install it](https://grpc.io/docs/protoc-installation/#install-using-a-package-manager).

You will also need to run:

```bash
$ chmod a+x ./run.sh
...
$ python3.10 -m pip install -r requirements.txt
...
```

### Run Python Dataform

To compile an example project, and see the full compiled graph:

```bash
./run.sh compile -d=examples/stackoverflow_bigquery --json
```

To see all CLI options, use:

```bash
./run.sh
```

### Why not Bazel?

We've decided to remove Bazel for this version (at lease for now) because:

- Using Bazel increases the activation energy for external contributors.
- The complexity of this project is far simpler, so we gain less benefit.
- We no longer need a tight integration with the legacy Dataform web app, which used Bazel.

## Feature Equivalence Progress

| Feature         | Description                   |
| --------------- | ----------------------------- |
| Core            | 🔵🔵🔵🔵⚪️⚪️⚪️⚪️⚪️⚪️    |
| CLI             | 🔵⚪️⚪⚪️⚪️⚪️⚪️⚪️⚪️⚪️  |
| Package Support | ⚪️⚪️⚪⚪️⚪️⚪️⚪️⚪️⚪️⚪️ |
