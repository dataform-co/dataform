#!/bin/bash

bazel build github-issue-webhook
bazel build github-issue-webhook:package

gcloud functions deploy handleStackdriverEvent \
  --source=bazel-bin/github-issue-webhook \
  --runtime nodejs10 \
  --trigger-http
