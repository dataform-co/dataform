To publish new versions, you will need bazel installed and the correct vsce key.

- In the main repo run `bazel run vscode:packager /tmp/dataform-package.vsix`
- Then take the package and paste it into this directly
- Then run `vsce publish`
