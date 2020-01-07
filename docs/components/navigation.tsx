import { Icon } from "@blueprintjs/core";
import { IFileTree } from "df/docs/cms";
import * as styles from "df/docs/components/navigation.css";
import * as React from "react";

interface IProps {
  version: string;
  tree: IFileTree;
  currentPath: string;
}

export default class Navigation extends React.Component<IProps> {
  public render() {
    return <div className={styles.navigation}>{this.renderTrees(this.props.tree.children)}</div>;
  }
  private renderTrees(trees: IFileTree[], depth = 0) {
    trees.sort((a: IFileTree, b: IFileTree) =>
      a.attributes.priority == null && b.attributes.priority == null
        ? a.attributes.title > b.attributes.title
          ? 1
          : -1
        : !(a.attributes.priority == null || b.attributes.priority == null)
        ? a.attributes.priority - b.attributes.priority
        : a.attributes.priority == null
        ? 1
        : -1
    );

    return (
      <ul className={styles[`depth${depth}`]}>
        {trees.map(tree => {
          const classNames = [styles[`depth${depth}`]];
          if (this.props.currentPath === tree.file.path) {
            classNames.push(styles.active);
          }
          const hasChildren = tree.children && tree.children.length > 0;
          if (hasChildren) {
            classNames.push(styles.hasChildren);
          }

          return (
            <React.Fragment key={tree.file.path}>
              <li className={classNames.join(" ")}>
                <a
                  href={`/${this.props.version ? `v/${this.props.version}/` : ""}${tree.file.path}`}
                >
                  {tree.attributes.title}
                  {depth > 0 && hasChildren && <Icon icon="chevron-right" />}
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
