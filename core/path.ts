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

export function fileName(fullPath: string) {
  return fullPath
    .split(separator)
    .slice(-1)[0]
    .split(".")[0];
}

export function escapedFileName(path: string) {
  return fileName(path).replace(/\\/g, "\\\\");
}

export function fileExtension(fullPath: string) {
  return fullPath.split(".").slice(-1)[0];
}
