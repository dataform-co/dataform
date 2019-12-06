export interface IFile {
  path: string;
  hasChildren: boolean;
}

export interface IFrontMatter {
  title?: string;
}

export interface IFileTree {
  file: IFile;
  attributes: IFrontMatter;
  content: string;
  children?: IFileTree[];
}

export interface ICms {
  list(path?: string): Promise<IFile[]>;
  get(path?: string): Promise<string>;
}
