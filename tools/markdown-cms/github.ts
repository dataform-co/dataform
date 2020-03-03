import { ICms } from "@dataform-tools/markdown-cms/index";
import { Tree } from "@dataform-tools/markdown-cms/tree";
import * as Octokit from "@octokit/rest";
import { join } from "path";

const octokit = new Octokit({ auth: process.env.GITHUB_AUTH_TOKEN });

interface IOptions {
  owner: string;
  repo: string;
  rootPath: string;
  ref: string;
}

export class GitHubCms<T> implements ICms<T> {
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

    const cleanPath = path.endsWith(".md") ? path.substring(0, path.length - 3) : path;
    const tree = Tree.create<T>(
      cleanPath,
      await this.content(actualFilePath).catch(e => ""),
      `https://github.com/${this.options.owner}/${this.options.repo}/blob/master/${actualFilePath}`
    );

    if (result.data instanceof Array) {
      const files = result.data.filter(file => file.name !== "index.md");
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
