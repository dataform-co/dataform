export const separator = "/";

/**
 * Replaces every backslash with a forward slash. Repeated separators are deliberately kept, so a
 * UNC root such as `\\server\share\proj` becomes `//server/share/proj` rather than a POSIX-looking
 * `/server/share/proj`.
 */
export function toPosixPath(path: string): string {
  return path ? path.replace(/\\/g, "/") : path;
}

const WINDOWS_DRIVE_LETTER = /^[a-zA-Z]:(?=\/|$)/;

/**
 * Normalizes a path for comparison purposes: forward slashes and a lower-cased Windows drive
 * letter, so that `C:/proj` and `c:/proj` compare equal. Everything else keeps its case, because
 * POSIX file systems are case-sensitive.
 */
export function comparablePath(path: string): string {
  const posixPath = toPosixPath(path);
  return WINDOWS_DRIVE_LETTER.test(posixPath)
    ? posixPath[0].toLowerCase() + posixPath.slice(1)
    : posixPath;
}

/**
 * Returns true if `path` is `base` itself or is located inside `base`. Only Windows drive letters
 * are compared case-insensitively.
 */
export function startsWithPath(path: string, base: string): boolean {
  if (!path || !base) {
    return false;
  }
  const withTrailingSlash = (p: string) => (p.endsWith("/") ? p : p + "/");
  return withTrailingSlash(comparablePath(path)).startsWith(
    withTrailingSlash(comparablePath(base)),
  );
}

/**
 * Returns `fullPath` relative to `base`, using forward slashes:
 * - `definitions/a.sqlx` for a path inside `base`,
 * - `""` when `fullPath` is `base` itself,
 * - the normalized `fullPath` unchanged when `base` is empty or `fullPath` lies outside of it.
 */
export function relativePath(fullPath: string, base: string) {
  const normalizedFull = toPosixPath(fullPath);
  if (base.length === 0) {
    return normalizedFull;
  }
  const normalizedBase = toPosixPath(base);
  if (!startsWithPath(normalizedFull, normalizedBase)) {
    return normalizedFull;
  }
  const stripped = normalizedFull.slice(normalizedBase.length);
  return stripped.startsWith("/") ? stripped.slice(1) : stripped;
}

export function filename(path: string) {
  return toPosixPath(path).split("/").slice(-1)[0];
}

export function basename(path: string) {
  const f = filename(path);
  const dotIndex = f.lastIndexOf(".");
  return dotIndex <= 0 ? f : f.substring(0, dotIndex);
}

export function dirName(fullPath: string) {
  const normalized = toPosixPath(fullPath);
  const lastSlash = normalized.lastIndexOf("/");
  if (lastSlash === -1) {
    return "";
  }
  if (lastSlash === 0 || /^[a-zA-Z]:$/.test(normalized.slice(0, lastSlash))) {
    return normalized.slice(0, lastSlash + 1);
  }
  return normalized.slice(0, lastSlash);
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

export function fileExtension(fullPath: string) {
  const f = filename(fullPath);
  const dotIndex = f.lastIndexOf(".");
  return dotIndex <= 0 ? "" : f.slice(dotIndex + 1);
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
