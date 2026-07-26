#!/bin/bash
set -e

yarn install

bazel build //packages/@dataform/cli:bin

bazel run packages/@dataform/cli:package.pack
bazel run packages/@dataform/core:package.pack

mv dataform-cli-3.0.59.tgz ../hephaestus-worker-base/dataform/dataform-cli-3.0.59.tgz
mv dataform-core-3.0.59.tgz ../hephaestus-worker-base/dataform/dataform-core-3.0.59.tgz
