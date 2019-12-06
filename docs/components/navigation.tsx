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
  private renderTrees(trees: IFileTree[]) {
    return trees.map(tree =>
      tree.children && tree.children.length > 0 ? (
        <React.Fragment>
          <h4>{tree.attributes.title}</h4>
          <Menu className={styles.menu}>{this.renderTrees(tree.children)}</Menu>
        </React.Fragment>
      ) : (
        <MenuItem
          text={tree.attributes.title}
          href={`/${this.props.version ? `v/${this.props.version}/` : ""}${tree.file.path}`}
        />
      )
    );
  }
}
