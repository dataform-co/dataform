(function() {
  const Module = require('module');
  const path = require('path');

  let runfilesDir = process.env.RUNFILES;
  if (!runfilesDir) {
    const mainFilename = require.main ? require.main.filename : __filename;
    const runfilesIndex = mainFilename.indexOf('.runfiles');
    if (runfilesIndex !== -1) {
      runfilesDir = mainFilename.substring(0, runfilesIndex + 9);
    }
  }
  
  if (runfilesDir) {
    runfilesDir = path.resolve(runfilesDir);
  }
  
  const originalResolveFilename = Module._resolveFilename;
  
  Module._resolveFilename = function (request, parent, isMain, options) {
    if (request === 'df' || request.startsWith('df/')) {
      if (runfilesDir) {
        const relativePath = request === 'df' ? '' : request.substring(3);
        const resolvedPath = path.join(runfilesDir, '_main', relativePath);
        try {
          return originalResolveFilename(resolvedPath, parent, isMain, options);
        } catch (e) {
          try {
            return originalResolveFilename(path.join(runfilesDir, relativePath), parent, isMain, options);
          } catch (e2) {
            // ignore
          }
          throw e;
        }
      }
    }
    try {
      return originalResolveFilename(request, parent, isMain, options);
    } catch (err) {
      if (runfilesDir && !request.startsWith('.') && !path.isAbsolute(request)) {
        try {
          return originalResolveFilename(path.join(runfilesDir, '_main', 'node_modules', request), parent, isMain, options);
        } catch (err2) {
          try {
            return originalResolveFilename(path.join(runfilesDir, 'node_modules', request), parent, isMain, options);
          } catch (err3) {
            // ignore
          }
        }
      }
      throw err;
    }
  }
})();
