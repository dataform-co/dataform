import { Button, Icon } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/docs/components/navigation.css";
import { IExtraAttributes } from "df/docs/content_tree";
import { ITree } from "df/tools/markdown-cms/tree";

interface IProps {
  version: string;
  tree: ITree<IExtraAttributes>;
  currentPath: string;
}

interface IState {
  expandedPaths: string[];
}

export default class Navigation extends React.Component<IProps, IState> {
  constructor(props: IProps) {
    super(props);
    this.state = { expandedPaths: [] };
  }
  public render() {
    return <div className={styles.navigation}>{this.renderTrees(this.props.tree.children)}</div>;
  }
  private renderTrees(trees: Array<ITree<IExtraAttributes>>, depth = 0) {
    return (
      <ul className={styles[`depth${depth}`]}>
        {trees.map(tree => {
          if (tree.attributes.redirect || !tree.attributes.title) {
            return null;
          }
          const classNames = [styles[`depth${depth}`]];
          const active =
            (!!tree.path && this.props.currentPath.includes(tree.path)) ||
            (!tree.path && !this.props.currentPath);
          if (active) {
            classNames.push(styles.active);
          }
          const hasChildren = tree.children && tree.children.length > 0;
          if (hasChildren) {
            classNames.push(styles.hasChildren);
          }

          return (
            <React.Fragment key={tree.path}>
              <li className={classNames.join(" ")}>
                <a href={getLink(tree.path, this.props.version)} className={styles.title}>
                  <div className={styles.icon}>
                    {" "}
                    {tree.attributes.icon && (
                      <Icon iconSize={20} icon={tree.attributes.icon as any} />
                    )}
                  </div>
                  <div>{tree.attributes.title}</div>
                </a>
                {tree.children?.length > 0 && !active && (
                  <Button
                    className={styles.expandButton}
                    minimal={true}
                    icon={
                      this.state.expandedPaths.includes(tree.path) ? "chevron-up" : "chevron-down"
                    }
                    onClick={() =>
                      this.setState(state => {
                        if (state.expandedPaths.includes(tree.path)) {
                          this.setState({
                            expandedPaths: state.expandedPaths.filter(path => path !== tree.path)
                          });
                        } else {
                          this.setState({ expandedPaths: [...state.expandedPaths, tree.path] });
                        }
                      })
                    }
                  />
                )}
              </li>
              {tree.children &&
                tree.children.length > 0 &&
                (this.props.currentPath.includes(tree.path) ||
                  (!this.props.currentPath && tree.path.startsWith("introduction")) ||
                  this.state.expandedPaths.includes(tree.path)) &&
                this.renderTrees(tree.children, depth + 1)}
            </React.Fragment>
          );
        })}
      </ul>
    );
  }
}

export function getLink(path: string, version: string) {
  return `/${version ? `v/${version}/` : ""}${path}`;
}
