There are two routes to publishing new versions:

### Web (actually working):

- In the main repo run `bazel run vscode:packager /tmp/dataform-package.vsix`
- Take the generated package and upload it [here](https://marketplace.visualstudio.com/manage/publishers/dataform)

### CLI (not working right now)

- In the main repo run `bazel run vscode:packager /tmp/dataform-package.vsix`
- Then take the package and paste it into this directory
- Then run `vsce publish`
  - Currently this doesn't work as `vsce publish` will try to publish typescript files
