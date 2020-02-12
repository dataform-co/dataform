import { Button } from "@blueprintjs/core";
import { ITree } from "df/docs/cms/tree";
import Navigation from "df/docs/components/navigation";
import { IHeaderLink, PageLinks } from "df/docs/components/page_links";
import { BaseLayout } from "df/docs/layouts/base";
import * as styles from "df/docs/layouts/documentation.css";
import * as React from "react";

export interface IProps {
  version: string;
  index: ITree;
  current: ITree;
  headerLinks?: IHeaderLink[];
}

export default class Documentation extends React.Component<IProps> {
  public getHeaderLinks(): IHeaderLink[] {
    return React.Children.toArray(this.props.children || [])
      .map(child => child as React.ReactElement<any>)
      .filter(child => !!child.props.children)
      .map(child =>
        React.Children.toArray(child.props.children)
          .map(child => child as React.ReactElement<any>)
          .filter(child => child.type === "h2")
          .map(child => ({
            id: child.props.id,
            text: child.props.children[0]
          }))
      )
      .reduce((acc, curr) => [...acc, ...curr], []);
  }

  public render() {
    const currentHeaderLinks = this.props.headerLinks || this.getHeaderLinks();
    return (
      <BaseLayout title={`Dataform docs | ${this.props.current.attributes.title}`}>
        <div className={styles.container}>
          <div className={styles.sidebar}>
            <Navigation
              currentPath={this.props.current.path}
              version={this.props.version}
              tree={this.props.index}
            />
          </div>
          <div className={styles.mainContent}>
            <div className={styles.titleBlock}>
              <h1>{this.props.current.attributes.title}</h1>
            </div>
            {this.props.children}
          </div>
          <div className={styles.sidebarRight}>
            <div className={styles.titleRight}>
              {this.props.current.editLink && (
                <a href={this.props.current.editLink}>
                  <Button text="Suggest edits" rightIcon={"annotation"} minimal={true} />
                </a>
              )}
            </div>
            <PageLinks links={currentHeaderLinks} />
          </div>
        </div>
      </BaseLayout>
    );
  }
}
