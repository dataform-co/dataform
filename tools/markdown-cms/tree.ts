import frontMatter from "front-matter";
import { basename, join } from "path";

export interface IBaseAttributes {
  title: string;
  subtitle?: string;
  priority?: number;
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

  public static createFromIndex<T>(index: ITree<T>): Tree<T> {
    return new Tree<T>(
      index.path,
      "",
      index.attributes,
      index.editLink,
      (index.children || []).map(child => Tree.createFromIndex(child))
    );
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
    this.sort();
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
    this.sort();
    return this;
  }

  public index(): ITree<T> {
    return {
      ...this,
      content: undefined,
      children: this.children.map(child => child.index())
    };
  }

  public sort() {
    this.children.sort((a: ITree<T>, b: ITree<T>) =>
      a.attributes.priority == null && b.attributes.priority == null
        ? // If no priorities, compare titles.
          a.attributes.title > b.attributes.title
          ? 1
          : -1
        : !(a.attributes.priority == null || b.attributes.priority == null)
        ? // If both have priorities set, compare priority.
          a.attributes.priority - b.attributes.priority
        : // Take any priority set as higher priority than one without.
        a.attributes.priority == null
        ? 1
        : -1
    );
    this.children.forEach(child => child.sort());
  }
}
