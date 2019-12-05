export interface IFile {
  path: string;
  hasChildren: boolean;
}

export interface IFrontMatter {
  title?: string;
}

export interface IFileTree {
  path: string;
  attributes: IFrontMatter;
  children?: IFileTree[];
}

export interface ICms {
  list(version: string, path?: string): Promise<IFile[]>;
  get(version: string, path?: string): Promise<string>;
}
