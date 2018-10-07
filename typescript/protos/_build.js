rule({
  name: "protos",
  cache: "../../protos/**/*.proto",
  build: ctx => [
    `chmod +x ../../node_modules/protobufjs/cli/bin/pbjs`,
    `chmod +x ../../node_modules/protobufjs/cli/bin/pbts`,
    `../../node_modules/protobufjs/cli/bin/pbjs -t static-module -w commonjs \
      -p ../../ \
      -o index.js \
      protos/core.proto \
      protos/profiles.proto`,
    `../../node_modules/protobufjs/cli/bin/pbts -o index.d.ts index.js`
  ],
  deps: ["//typescript:lerna_bootstrap"]
});
