export const separator = "/";

export function toPosixPath(path: string): string {
  return path ? path.replace(/\\+/g, "/") : path;
}

export function relativePath(fullPath: string, base: string) {
  const normalizedFull = toPosixPath(fullPath);
  if (base.length === 0) {
    return normalizedFull;
  }
  let normalizedBase = toPosixPath(base);
  if (!normalizedBase.endsWith("/")) {
    normalizedBase += "/";
  }
  if (normalizedFull.toLowerCase().startsWith(normalizedBase.toLowerCase())) {
    return normalizedFull.slice(normalizedBase.length);
  }
  return normalizedFull;
}

export function filename(path: string) {
  return toPosixPath(path).split("/").slice(-1)[0];
}

export function basename(path: string) {
  const f = filename(path);
  const dotIndex = f.lastIndexOf(".");
  return dotIndex === -1 ? f : f.substring(0, dotIndex);
}

export function dirName(fullPath: string) {
  const normalized = toPosixPath(fullPath);
  const lastSlash = normalized.lastIndexOf("/");
  return lastSlash === -1 ? "" : normalized.slice(0, lastSlash);
}

export function join(...paths: string[]) {
  return paths
    .map(toPosixPath)
    .map((path) => {
      if (path.startsWith(separator)) {
        path = path.slice(1);
      }
      if (path.endsWith(separator)) {
        path = path.slice(0, -1);
      }
      return path;
    })
    .filter((path) => path.length > 0)
    .join(separator);
}

export function escapedBasename(path: string) {
  return basename(path).replace(/\\/g, "\\\\");
}

export function fileExtension(fullPath: string) {
  const f = filename(fullPath);
  const dotIndex = f.lastIndexOf(".");
  return dotIndex === -1 ? "" : f.slice(dotIndex + 1);
}

export function normalize(path: string) {
  const normalized = toPosixPath(path);
  const parts = [];
  let dotDotCount = 0;
  for (const part of normalized.split("/").filter((p) => !!p && p !== ".")) {
    if (part === "..") {
      if (parts.length === 0) {
        dotDotCount++;
      } else {
        parts.pop();
      }
    } else {
      parts.push(part);
    }
  }
  if (normalized.startsWith("/")) {
    if (parts.length === 0) {
      return "/";
    }
    parts.unshift("");
  } else {
    parts.unshift(...new Array(dotDotCount).fill(".."));
    if (parts.length === 0) {
      return ".";
    }
  }
  return parts.join("/");
}
