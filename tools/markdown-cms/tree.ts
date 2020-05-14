import frontMatter from "front-matter";
import { basename, join } from "path";

export interface IBaseAttributes {
  title: string;
}

type IWithBaseAttributes<T> = T & IBaseAttributes;

export interface ITree<T> {
  readonly path: string;
  readonly name: string;
  readonly content: string;
  readonly attributes?: IWithBaseAttributes<T>;
  readonly children?: Array<ITree<T>>;
  editLink?: string;
}

export class Tree<T> implements ITree<T> {
  public static create<T>(path: string, rawContent: string, editLink?: string): Tree<T> {
    let content: string = null;
    let attributes: IBaseAttributes = { title: basename(path) };

    const parsedContent = frontMatter(rawContent);
    content = parsedContent.body;
    attributes = parsedContent.attributes as IBaseAttributes;

    return new Tree(path, content, attributes as any, editLink, []);
  }
  public readonly name: string;

  constructor(
    public readonly path: string,
    public readonly content: string,
    public readonly attributes?: IWithBaseAttributes<T>,
    public readonly editLink?: string,
    public readonly children: Array<Tree<T>> = []
  ) {
    const base = basename(path);
    this.name = base.includes(".md") ? base.substring(0, base.length - 3) : base;
  }

  public getChild(path: string): Tree<T> {
    const pathParts = path.split("/");
    return pathParts.reduce(
      (acc: Tree<T>, curr: string) =>
        !curr ? acc : acc?.children.find(child => child.path === join(acc.path, curr)),
      this
    );
  }

  public addChild(child: Tree<T>) {
    this.children.push(child);
    return this;
  }

  public index(): ITree<T> {
    return {
      ...this,
      content: undefined,
      children: this.children.map(child => child.index())
    };
  }
}
