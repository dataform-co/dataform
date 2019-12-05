import { ICms, IFile } from "df/docs/cms";
import * as fs from "fs";
import { join } from "path";
import { promisify } from "util";

export class LocalCms implements ICms {
  constructor(private rootPath: string) {}

  public async list(version: string, directoryPath?: string) {
    const isDir = await this.isDirectory(directoryPath);
    if (!isDir) {
      return [];
    }
    const files = (await promisify(fs.readdir)(join(this.rootPath, directoryPath))).filter(
      file => !file.endsWith("index.md")
    );
    return Promise.all(
      files.map(async filePath => {
        const fullPath = join(directoryPath, filePath);
        const cleanPath = fullPath.endsWith(".md")
          ? fullPath.substring(0, fullPath.length - 3)
          : fullPath;
        const stat = await promisify(fs.stat)(join(this.rootPath, fullPath));
        return { path: cleanPath, hasChildren: stat.isDirectory() };
      })
    );
  }

  public async get(version: string, filePath?: string) {
    const isDir = await this.isDirectory(filePath);
    const resolvedPath = isDir ? join(filePath, "index.md") : `${filePath}.md`;
    return await promisify(fs.readFile)(join(this.rootPath, resolvedPath), "utf8");
  }

  private async isDirectory(path: string) {
    try {
      const stat = await promisify(fs.stat)(join(this.rootPath, path));
      return stat.isDirectory();
    } catch (e) {
      // Doesn't exist, so can't be a directory.
      return false;
    }
  }
}
