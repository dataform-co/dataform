#!/bin/bash
# Temporary script for making typedocs.
bazel build core protos
bazel run @nodejs//:yarn add typedoc
npx typedoc --out typedoc/ --excludePrivate --excludeNotExported --ignoreCompilerErrors --entryPoint \"table\" --includeDeclarations  --excludeExternals --externalPattern "**/node_modules/**" core/ --json test.json --name "Dataform API Reference" --theme="minimal"
