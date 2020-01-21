import { ICms } from "df/docs/cms/index";
import { Tree } from "df/docs/cms/tree";
import { join } from "path";

const Octokit = require("@octokit/rest");

const octokit = new Octokit({ auth: process.env.GITHUB_OAUTH_TOKEN });

interface IOptions {
  owner: string;
  repo: string;
  rootPath: string;
  ref: string;
}

export class GitHubCms implements ICms {
  constructor(private options: IOptions) {}

  public async get(path = "") {
    const result = await octokit.repos.getContents({
      owner: this.options.owner,
      repo: this.options.repo,
      ref: this.options.ref,
      path: join(this.options.rootPath, path)
    });

    const isDir = result.data instanceof Array;
    const actualFilePath = isDir
      ? join(this.options.rootPath, path, "index.md")
      : join(this.options.rootPath, path);

    const tree = Tree.create(path, await this.content(actualFilePath).catch(e => ""));

    if (isDir) {
      const files: any[] = result.data.filter(file => file.name !== "index.md");
      const children = await Promise.all(files.map(async file => this.get(join(path, file.name))));
      children.forEach(child => tree.addChild(child));
    }

    return tree;
  }

  public async content(path: string): Promise<string> {
    return await octokit.repos
      .getContents({
        owner: this.options.owner,
        repo: this.options.repo,
        ref: this.options.ref,
        path
      })
      .then(result => {
        const buffer = new Buffer((result.data as any).content, "base64");
        return buffer.toString("utf8");
      });
  }
}
