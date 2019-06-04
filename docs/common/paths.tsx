export function pathFromFilename(filename: string) {
  if (!!filename && filename.indexOf("pages/") >= 0) {
    return `/${filename.split("pages/")[1].split(".")[0]}`;
  }
  return filename;
}
