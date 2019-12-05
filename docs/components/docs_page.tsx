import rehypePrism from "@mapbox/rehype-prism";
import * as Octokit from "@octokit/rest";
import { ICms, IFile, IFileTree, IFrontMatter } from "df/docs/cms";
import { GitHubCms } from "df/docs/cms/github";
import { LocalCms } from "df/docs/cms/local";
import Documentation from "df/docs/layouts/documentation";
import * as frontMatter from "front-matter";
import { NextPageContext } from "next";
import { basename } from "path";
import * as React from "react";
import rehypeRaw from "rehype-raw";
import rehypeReact from "rehype-react";
import rehypeSlug from "rehype-slug";
import remark from "remark";
import remarkRehype from "remark-rehype";
import { promisify } from "util";

const octokit = new Octokit();

interface IQuery {
  version: string;
  path0: string;
  path1: string;
}

interface IProps {
  attributes: IFrontMatter;
  content?: string;
  tree: IFileTree;
}

const localCms = new LocalCms("content/docs");
const gitHubCms = new GitHubCms({
  owner: "dataform-co",
  repo: "dataform",
  rootPath: "content/docs"
});

export class DocsPage extends React.Component<IProps> {
  public static async getInitialProps({
    query
  }: NextPageContext & { query: IQuery }): Promise<IProps> {
    const cms = !query.version || query.version === "local" ? localCms : gitHubCms;

    const queryPath = [query.path0, query.path1].filter(part => !!part).join("/");

    const tree = await computeTree(cms, query.version, "");
    console.log(JSON.stringify(tree, null, 2));

    const parsedMarkdown = frontMatter(await cms.get(query.version, queryPath));
    const attributes = parsedMarkdown.attributes as IFrontMatter;
    return { tree, attributes, content: parsedMarkdown.body };
  }

  public render() {
    return (
      <Documentation attributes={this.props.attributes} tree={this.props.tree}>
        {this.props.content &&
          remark()
            .use(remarkRehype, { allowDangerousHTML: true })
            .use(rehypeSlug)
            .use(rehypePrism)
            .use(rehypeRaw)
            .use(rehypeReact, { createElement: React.createElement })
            .processSync(this.props.content).contents}
      </Documentation>
    );
  }
}

async function computeTree(cms: ICms, version: string, path: string): Promise<IFileTree> {
  const children = await cms.list(version, path);

  console.log(children);

  const getAttributes = async (path: string) => {
    try {
      const content = await cms.get(version, path);
      return frontMatter(content).attributes;
    } catch (e) {
      return { title: basename(path) } as IFrontMatter;
    }
  };

  return {
    path,
    attributes: await getAttributes(path),
    children: await Promise.all(children.map(child => computeTree(cms, version, child.path)))
  };
}
