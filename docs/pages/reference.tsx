import { Typedoc } from "df/docs/components/typedoc";
import Documentation from "df/docs/layouts/documentation";
import { readFile } from "fs";
import { NextPageContext } from "next";
import * as React from "react";
import { ContainerReflection } from "typedoc";
import { promisify } from "util";

interface IQuery {}

interface IProps {
  typedoc: ContainerReflection;
}

export class Reference extends React.Component<IProps> {
  public static async getInitialProps({
    query
  }: NextPageContext & { query: IQuery }): Promise<IProps> {
    const typedocFile = await promisify(readFile)("docs/core.typedoc.json", "utf8");
    const typedoc: ContainerReflection = JSON.parse(typedocFile);

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
        current={{ file: { path: "/reference", hasChildren: false }, attributes: {} }}
        index={{
          file: { path: "/reference", hasChildren: false },
          attributes: { title: "API Reference" },
          content: "",
          children: []
        }}
        headerLinks={Typedoc.getHeaderLinks(typedocProps)}
      >
        <Typedoc {...typedocProps} />
      </Documentation>
    );
  }
}

export default Reference;
