import { Tree } from "df/tools/markdown-cms/tree";

export interface ICms {
  get(): Promise<Tree>;
}
