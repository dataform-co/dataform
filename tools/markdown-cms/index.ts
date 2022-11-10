import { Tree } from "df/tools/markdown-cms/tree";

export interface ICms<T> {
  get(): Promise<Tree<T>>;
}
