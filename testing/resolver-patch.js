// This patching is necessary to ensure "df/..." imports are properly resolved at runtime during testing
// and development (tsconfig.json "paths" handles import resolution at TS compile-time, and the built package
// everything in a single .js bundle)

(function () {
  const Module = require("module");
  const path = require("path");

  const DF_PREFIX = "df/";
  const mainRunfilesDir = process.env.RUNFILES
    ? path.resolve(process.env.RUNFILES, "_main")
    : undefined;

  const originalResolveFilename = Module._resolveFilename;

  Module._resolveFilename = function (request, parent, isMain, options) {
    if (mainRunfilesDir && (request === "df" || request.startsWith(DF_PREFIX))) {
      const relativePath = request === "df" ? "" : request.substring(DF_PREFIX.length);
      return originalResolveFilename(
        path.join(mainRunfilesDir, relativePath),
        parent,
        isMain,
        options,
      );
    }
    try {
      return originalResolveFilename(request, parent, isMain, options);
    } catch (err) {
      if (mainRunfilesDir && !request.startsWith(".") && !path.isAbsolute(request)) {
        try {
          return originalResolveFilename(
            path.join(mainRunfilesDir, "node_modules", request),
            parent,
            isMain,
            options,
          );
        } catch (err2) {
          // ignore, throw original err
        }
      }
      throw err;
    }
  };
})();
