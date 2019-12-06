import * as Octokit from "@octokit/rest";
import { ICms, IFile } from "df/docs/cms";
import { join } from "path";

const octokit = new Octokit({ auth: "5e396250f7b4dd25c3519278faf28a447573aad0" });

interface IOptions {
  owner: string;
  repo: string;
  rootPath: string;
}

export class GitHubCms implements ICms {
  constructor(private options: IOptions) {}

  public async list(version: string, directoryPath?: string) {
    const isDir = this.isDirectory(version, directoryPath);
    if (!isDir) {
      return [];
    }
    const result = await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: version,
      path: directoryPath
    });
    console.log(result);
    // TODO
    return [] as IFile[];
  }

  public async get(version: string, filePath?: string) {
    const isDir = this.isDirectory(version, filePath);
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
    const result = await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: version,
      path: join(this.options.rootPath, filePath)
    });
    return (result.data as any).type !== "file";
  }
}
