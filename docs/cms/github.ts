import { ICms, IFile } from "df/docs/cms/index";
import { join } from "path";

const Octokit = require("@octokit/rest");

const octokit = new Octokit({ auth: "" });

interface IOptions {
  owner: string;
  repo: string;
  rootPath: string;
  ref: string;
}

export class GitHubCms implements ICms {
  constructor(private options: IOptions) {}

  public async list(directoryPath?: string) {
    const isDir = await this.isDirectory(directoryPath);
    if (!isDir) {
      return [];
    }
    const result = await this.getContents(directoryPath);

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
        return {
          path: cleanPath,
          contentPath: fullPath,
          hasChildren: file.type === "dir"
        } as IFile;
      });
  }

  public async get(filePath?: string) {
    const isDir = await this.isDirectory(filePath);
    const resolvedPath = isDir ? join(filePath, "index.md") : `${filePath}.md`;
    const result = await this.getContents(resolvedPath);
    const buffer = new Buffer((result.data as any).content, "base64");
    return buffer.toString("utf8");
  }

  private async getContents(path: string) {
    return await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: this.options.ref,
      path: join(this.options.rootPath, path)
    });
  }

  private async isDirectory(filePath?: string): Promise<boolean> {
    try {
      const result = await this.getContents(filePath);
      return result.data instanceof Array;
    } catch (e) {
      return false;
    }
  }
}
