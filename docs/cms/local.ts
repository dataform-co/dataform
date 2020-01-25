import { ICms } from "df/docs/cms";
import { Tree } from "df/docs/cms/tree";
import * as fs from "fs";
import { basename, dirname, join } from "path";
import { promisify } from "util";

export class LocalCms implements ICms {
  constructor(private rootPath: string) {}

  public async get(path = "") {
    const isDir = await this.isDirectory(path);
    const actualFilePath = isDir
      ? join(this.rootPath, path, "index.md")
      : join(this.rootPath, path);
    const rawContent = await promisify(fs.readFile)(actualFilePath, "utf8");

    const cleanPath = path.endsWith(".md") ? path.substring(0, path.length - 3) : path;
    const tree = Tree.create(
      cleanPath,
      rawContent,
      `https://github.com/dataform-co/dataform/blob/master/${actualFilePath}`
    );

    if (isDir) {
      const files = (await promisify(fs.readdir)(join(this.rootPath, path))).filter(
        file => !file.endsWith("index.md")
      );
      const children = await Promise.all(
        files.map(async filePath => this.get(join(path, filePath)))
      );
      children.forEach(child => tree.addChild(child));
    }
    return tree;
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
