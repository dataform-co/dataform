import { ICms, IFileTree, IFrontMatter } from "df/docs/cms";
import { basename, join } from "path";

const frontMatter = require("front-matter");

export class Tree {
  public static async create(cms: ICms, path = ""): Promise<Tree> {
    return new Tree(await Tree.build(cms, path));
  }
  private static async build(cms: ICms, path: string): Promise<IFileTree> {
    const children = await cms.list(path);

    let content: string = null;
    let attributes: IFrontMatter = { title: basename(path) };

    try {
      const rawContent = await cms.get(path);
      const parsedContent = frontMatter(rawContent);
      content = parsedContent.body;
      attributes = parsedContent.attributes;
    } catch (e) {
      // File doesn't exist.
    }

    return {
      file: { path, hasChildren: children.length > 0 },
      content,
      attributes,
      children: await Promise.all(children.map(child => Tree.build(cms, child.path)))
    };
  }

  private constructor(private tree: IFileTree) {}

  public get(path = "") {
    if (!path) {
      return this.tree;
    }
    const pathParts = path.split("/");
    return pathParts.reduce(
      (acc: IFileTree, curr: string) =>
        !curr ? acc : acc.children.find(child => child.file.path === join(acc.file.path, curr)),
      this.tree
    );
  }

  /**
   * Returns a version of the tree without content, just metadata.
   */
  public index(): IFileTree {
    return {
      ...this.tree,
      content: undefined,
      children: this.tree.children.map(child => new Tree(child).index())
    };
  }
}
