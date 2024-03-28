export const separator = (() => {
  if (typeof process !== "undefined") {
    return process.platform === "win32" ? "\\" : "/";
  }
  return "/";
})();

export function relativePath(fullPath: string, base: string) {
  if (base.length === 0) {
    return fullPath;
  }
  const stripped = fullPath.substr(base.length);
  if (stripped.startsWith(separator)) {
    return stripped.substr(1);
  } else {
    return stripped;
  }
}

export function filename(path: string) {
  return path.split(separator).slice(-1)[0];
}

export function basename(path: string) {
  const f = filename(path);
  return f.substring(0, f.lastIndexOf("."));
}

export function dirName(fullPath: string) {
  return fullPath.slice(0, fullPath.lastIndexOf(separator));
}

export function join(...paths: string[]) {
  return paths
    .map(path => {
      if (path.startsWith(separator)) {
        path = path.slice(1);
      }
      if (path.endsWith(separator)) {
        path = path.slice(0, -1);
      }
      return path;
    })
    .join(separator);
}

export function escapedBasename(path: string) {
  return basename(path).replace(/\\/g, "\\\\");
}

export function fileExtension(fullPath: string) {
  return fullPath.split(".").slice(-1)[0];
}
