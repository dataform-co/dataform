import * as Octokit from "@octokit/rest";
import { ICms, IFile } from "df/docs/cms";
import { join } from "path";

const octokit = new Octokit({ auth: "" });

interface IOptions {
  owner: string;
  repo: string;
  rootPath: string;
}

export class GitHubCms implements ICms {
  constructor(private options: IOptions) {}

  public async list(version: string, directoryPath?: string) {
    const isDir = await this.isDirectory(version, directoryPath);
    console.log(join(this.options.rootPath, directoryPath));
    console.log(isDir);
    if (!isDir) {
      return [];
    }
    const result = await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: version,
      path: join(this.options.rootPath, directoryPath)
    });

    if (!(result.data instanceof Array)) {
      return [] as IFile[];
    }
    return result.data
      .filter(file => file.name !== "index.md")
      .map(file => {
        const fullPath = join(directoryPath, file.name);
        const cleanPath = fullPath.endsWith(".md")
          ? fullPath.substring(0, fullPath.length - 3)
          : fullPath;
        return { path: cleanPath, hasChildren: file.type === "dir" } as IFile;
      });
  }

  public async get(version: string, filePath?: string) {
    const isDir = await this.isDirectory(version, filePath);
    const resolvedPath = isDir ? join(filePath, "index.md") : `${filePath}.md`;
    const result = await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: version,
      path: join(this.options.rootPath, resolvedPath)
    });
    const buffer = new Buffer((result.data as any).content, "base64");
    return buffer.toString("utf8");
  }

  private async isDirectory(version: string, filePath?: string): Promise<boolean> {
    try {
      const result = await octokit.repos.getContents({
        owner: this.options.owner,
        repo: this.options.repo,
        ref: version,
        path: join(this.options.rootPath, filePath)
      });
      return result.data instanceof Array;
    } catch (e) {
      return false;
    }
  }
}
