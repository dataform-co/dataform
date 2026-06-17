(function() {
  const Module = require('module');
  const path = require('path');

  const RUNFILES_SUFFIX = '.runfiles';
  const DF_PREFIX = 'df/';

  let runfilesDir = process.env.RUNFILES;
  if (!runfilesDir) {
    const mainFilename = require.main ? require.main.filename : __filename;
    const runfilesIndex = mainFilename.indexOf(RUNFILES_SUFFIX);
    if (runfilesIndex !== -1) {
      runfilesDir = mainFilename.substring(0, runfilesIndex + RUNFILES_SUFFIX.length);
    }
  }
  
  if (runfilesDir) {
    runfilesDir = path.resolve(runfilesDir);
  }
  
  const originalResolveFilename = Module._resolveFilename;

  function tryResolve(paths, parent, isMain, options) {
    let firstError;
    for (const p of paths) {
      try {
        return originalResolveFilename(p, parent, isMain, options);
      } catch (e) {
        if (!firstError) {
          firstError = e;
        }
      }
    }
    throw firstError;
  }

  Module._resolveFilename = function (request, parent, isMain, options) {
    if (request === 'df' || request.startsWith(DF_PREFIX)) {
      if (runfilesDir) {
        const relativePath = request === 'df' ? '' : request.substring(DF_PREFIX.length);
        return tryResolve([
          path.join(runfilesDir, '_main', relativePath),
          path.join(runfilesDir, relativePath)
        ], parent, isMain, options);
      }
    }
    try {
      return originalResolveFilename(request, parent, isMain, options);
    } catch (err) {
      if (runfilesDir && !request.startsWith('.') && !path.isAbsolute(request)) {
        try {
          return tryResolve([
            path.join(runfilesDir, '_main', 'node_modules', request),
            path.join(runfilesDir, 'node_modules', request)
          ], parent, isMain, options);
        } catch (err2) {
          // ignore, throw original err
        }
      }
      throw err;
    }
  }
})();
