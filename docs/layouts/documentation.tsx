import { Button, Switch } from "@blueprintjs/core";
import { Card, CardActions, CardMasonry } from "df/components/card";
import { Footer } from "df/docs/components/footer";
import Navigation, { getLink } from "df/docs/components/navigation";
import { IHeaderLink, PageLinks } from "df/docs/components/page_links";
import { IExtraAttributes } from "df/docs/content_tree";
import { BaseLayout } from "df/docs/layouts/base";
import * as styles from "df/docs/layouts/documentation.css";
import { ITree, Tree } from "df/tools/markdown-cms/tree";
import * as React from "react";

export interface IProps {
  version: string;
  index: ITree<IExtraAttributes>;
  current: ITree<IExtraAttributes>;
  headerLinks?: IHeaderLink[];
}

export default class Documentation extends React.Component<IProps> {
  public getHeaderLinks(): IHeaderLink[] {
    return React.Children.toArray(this.props.children || [])
      .map(child => child as React.ReactElement<any>)
      .filter(child => !!child.props?.children)
      .map(child =>
        React.Children.toArray(child.props.children)
          .map(grandChild => grandChild as React.ReactElement<any>)
          .filter(grandChild => grandChild.type === "h2" || grandChild.type === "h3")
          .map(grandChild => ({
            id: grandChild.props.id,
            text: grandChild.props.children[0]
          }))
      )
      .reduce((acc, curr) => [...acc, ...curr], []);
  }

  public render() {
    const currentHeaderLinks = this.props.headerLinks || this.getHeaderLinks();
    const tree = Tree.createFromIndex<IExtraAttributes>(this.props.index);
    const current = tree.getChild(this.props.current.path);
    const pathParts = this.props.current.path.split("/");
    const parentPath = pathParts.slice(0, Math.max(pathParts.length - 1, 0)).join("/");
    const parent = tree.getChild(parentPath);
    const darkModeSaved =
      typeof localStorage !== "undefined" && localStorage.getItem("dataform-dark-mode") === "true";
    if (darkModeSaved) {
      document.body.classList.add("dark");
    }
    return (
      <BaseLayout title={`${this.props.current.attributes.title} | Dataform`}>
        <div className={styles.container}>
          <div className={styles.sidebar}>
            <Navigation
              currentPath={this.props.current.path}
              version={this.props.version}
              tree={this.props.index}
            />
            <div className={styles.flexFiller} />
            <div className={styles.darkMode}>
              <Switch
                defaultChecked={darkModeSaved}
                label="Dark mode"
                onClick={e => {
                  if (!e.currentTarget.checked) {
                    localStorage.setItem("dataform-dark-mode", "false");
                    document.body.classList.remove("dark");
                  } else {
                    localStorage.setItem("dataform-dark-mode", "true");
                    document.body.classList.add("dark");
                  }
                }}
              />
            </div>
          </div>
          <div className={styles.mainContent}>
            <div className={styles.titleBlock}>
              <h1>{this.props.current.attributes.title}</h1>
              <div className={styles.subheader}>{this.props.current.attributes.subtitle}</div>
            </div>
            {this.props.children}
            <div className={styles.next}>
              <h2>What's next</h2>
              <CardMasonry style={{ margin: "0px 0px 20px" }}>
                {(current.children?.length > 0 ? current.children : parent.children)
                  .filter(
                    child =>
                      !!child.path &&
                      !!child.attributes.title &&
                      !child.attributes?.redirect &&
                      current.path !== child.path
                  )
                  .map(child => (
                    <Card
                      masonryCard={true}
                      header={child.attributes?.title}
                      key={child.path}
                      className={styles.whatsNextCard}
                    >
                      <p>{child.attributes?.subtitle}</p>
                      <CardActions align="right">
                        <a href={getLink(child.path, this.props.version)}>
                          <Button minimal={true} text="Read more" />
                        </a>
                      </CardActions>
                    </Card>
                  ))}
              </CardMasonry>
            </div>
            <Footer tree={this.props.index} />
          </div>
          <div className={styles.sidebarRight}>
            <div className={styles.titleRight}>
              {this.props.current.editLink && (
                <a href={this.props.current.editLink}>✎ Edit this page on GitHub</a>
              )}
            </div>
            <PageLinks links={currentHeaderLinks} />
          </div>
        </div>
      </BaseLayout>
    );
  }
}
