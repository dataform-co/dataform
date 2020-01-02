import { ITypedoc, Typedoc } from "df/docs/components/typedoc";
import Documentation from "df/docs/layouts/documentation";
import { readFile } from "fs";
import * as React from "react";
import { promisify } from "util";

interface IProps {
  typedoc: ITypedoc;
}

export class Reference extends React.Component<IProps> {
  public static async getInitialProps(): Promise<IProps> {
    const typedocFile = await promisify(readFile)("docs/core.typedoc.json", "utf8");
    const typedoc: ITypedoc = JSON.parse(typedocFile);

    return { typedoc };
  }

  public render() {
    const typedocProps = {
      docs: this.props.typedoc,
      entry: [] as string[]
    };
    return (
      <Documentation
        version=""
        current={{
          file: { path: "/reference", hasChildren: false },
          attributes: { title: "Reference" }
        }}
        index={{ children: [] }}
        headerLinks={Typedoc.getHeaderLinks(typedocProps)}
      >
        <Typedoc {...typedocProps} />
      </Documentation>
    );
  }
}

export default Reference;
