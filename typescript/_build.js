rule({
  name: "lerna_bootstrap",
  build: "npx lerna bootstrap",
  cache: {
    include: "{lerna.json,*/package.json}",
  }
});
