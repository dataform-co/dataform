import { Tree } from "@dataform-tools/markdown-cms/tree";

export interface ICms<T> {
  get(): Promise<Tree<T>>;
}
