import { IFileTree } from "df/docs/cms";
import { ITypedoc, Typedoc } from "df/docs/components/typedoc";
import Documentation from "df/docs/layouts/documentation";
import { contentTree, localCms } from "df/docs/pages/_docs";
import { readFile } from "fs";
import { NextPageContext } from "next";
import * as React from "react";
import { promisify } from "util";

interface IQuery {
  version: string;
}

interface IProps {
  version: string;
  index: IFileTree;
  current: IFileTree;
  typedoc: ITypedoc;
}

export class Reference extends React.Component<IProps> {
  public static async getInitialProps(
    context: NextPageContext & { query: IQuery }
  ): Promise<IProps> {
    const typedocFile = await promisify(readFile)("docs/core.typedoc.json", "utf8");
    const typedoc: ITypedoc = JSON.parse(typedocFile);
    const tree = await contentTree(localCms);
    return {
      version: context.query.version || "local",
      index: tree.index(),
      current: tree.get("reference"),
      typedoc
    };
  }

  public render() {
    const typedocProps = {
      docs: this.props.typedoc,
      entry: [] as string[]
    };
    return (
      <Documentation
        version={this.props.version}
        current={this.props.current}
        index={this.props.index}
        headerLinks={Typedoc.getHeaderLinks(typedocProps)}
      >
        <Typedoc {...typedocProps} />
      </Documentation>
    );
  }
}

export default Reference;
