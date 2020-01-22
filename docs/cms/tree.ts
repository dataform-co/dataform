import { basename, join } from "path";

const frontMatter = require("front-matter");

export interface IFrontMatter {
  title?: string;
  priority?: number;
}

export interface ITree {
  readonly path: string;
  readonly name: string;
  readonly content: string;
  readonly attributes?: IFrontMatter;
  readonly children?: ITree[];
  editLink?: string;
}

export class Tree implements ITree {
  public static create(path: string, rawContent: string, editLink?: string): Tree {
    let content: string = null;
    let attributes: IFrontMatter = { title: basename(path) };

    const parsedContent = frontMatter(rawContent);
    content = parsedContent.body;
    attributes = parsedContent.attributes;

    return new Tree(path, content, attributes, editLink, []);
  }
  public readonly name: string;

  constructor(
    public readonly path: string,
    public readonly content: string,
    public readonly attributes?: IFrontMatter,
    public readonly editLink?: string,
    public readonly children: Tree[] = []
  ) {
    const base = basename(path);
    this.name = base.includes(".md") ? base.substring(0, base.length - 3) : base;
  }

  public getChild(path: string): Tree {
    const pathParts = path.split("/");
    return pathParts.reduce(
      (acc: Tree, curr: string) =>
        !curr ? acc : acc.children.find(child => child.path === join(acc.path, curr)),
      this
    );
  }

  public addChild(child: Tree) {
    this.children.push(child);
    return this;
  }

  public index(): ITree {
    return {
      ...this,
      content: undefined,
      children: this.children.map(child => child.index())
    };
  }
}
