#!/bin/bash
set -e

# Run tslint.
bazel run @nodejs//:yarn
bazel build @npm//tslint/bin:tslint && bazel-bin/external/npm/tslint/bin/tslint.sh --project .

# Run all the tests
bazel test --config=remote-cache ... --build_tests_only --test_env=USE_CLOUD_BUILD_NETWORK=true
