import { Menu, MenuItem } from "@blueprintjs/core";
import { IFileTree } from "df/docs/cms";
import * as styles from "df/docs/components/navigation.css";
import * as React from "react";

interface IProps {
  version: string;
  tree: IFileTree;
  currentPath: string;
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
        {trees.map(tree => {
          const classNames = [];
          if (this.props.currentPath === tree.file.path) {
            classNames.push(styles.active);
          }
          return (
            <React.Fragment key={tree.file.path}>
              <li
                className={classNames.join(" ")}
                style={{
                  fontWeight: this.props.currentPath.startsWith(tree.file.path) ? "bold" : "normal"
                }}
              >
                <a
                  href={`/${this.props.version ? `v/${this.props.version}/` : ""}${tree.file.path}`}
                >
                  {tree.attributes.title}
                </a>
              </li>
              {tree.children &&
                tree.children.length > 0 &&
                this.renderTrees(tree.children, depth + 1)}
            </React.Fragment>
          );
        })}
      </ul>
    );
  }
}
