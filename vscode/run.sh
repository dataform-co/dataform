bazel build vscode

# May Ben forgive me
cp $PWD/../bazel-bin/vscode/extension.js $PWD/extension.js
cp $PWD/../bazel-bin/vscode/server.js $PWD/server.js
chmod 777 $PWD/server.js
chmod 777 $PWD/extension.js

code --extensionDevelopmentPath=$PWD $@
