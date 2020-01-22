import { Tree } from "df/docs/cms/tree";

export interface ICms {
  get(): Promise<Tree>;
}
