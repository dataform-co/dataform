import { Menu, MenuItem } from "@blueprintjs/core";
import { IFileTree } from "df/docs/cms";
import * as styles from "df/docs/components/navigation.css";
import * as React from "react";

interface IProps {
  version: string;
  tree: IFileTree;
}

export interface IHeaderLink {
  id: string;
  text: string;
}

export default class Navigation extends React.Component<IProps> {
  public render() {
    return <div className={styles.navigation}>{this.renderTrees(this.props.tree.children)}</div>;
  }
  private renderTrees(trees: IFileTree[], depth = 0) {
    return (
      <ul>
        {trees.map(tree =>
          tree.children && tree.children.length > 0 ? (
            <React.Fragment>
              <li className={styles[`level${depth}`]}>{tree.attributes.title}</li>
              {this.renderTrees(tree.children, depth + 1)}
            </React.Fragment>
          ) : (
            <li>
              <a href={`/${this.props.version ? `v/${this.props.version}/` : ""}${tree.file.path}`}>
                {tree.attributes.title}
              </a>
            </li>
          )
        )}
      </ul>
    );
  }
}
