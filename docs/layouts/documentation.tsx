import Navigation, { IHeaderLink } from "df/docs/components/navigation";
import * as React from "react";

import { Button } from "@blueprintjs/core";
import { IFileTree, IFrontMatter } from "df/docs/cms";
import { BaseLayout } from "df/docs/layouts/base";
import * as styles from "df/docs/layouts/documentation.css";

export interface IProps {
  version: string;
  index: IFileTree;
  current: IFileTree;
}

export default class Documentation extends React.Component<IProps> {
  public getHeaderLinks(child: React.ReactElement<any>): IHeaderLink[] {
    if (child && child.props && Array.isArray(child.props.children)) {
      return (child.props.children as React.ReactElement[])
        .filter(item => item.props && item.props.name === "h2")
        .map(item => ({
          id: item.props.props.id,
          text: item.props.children
        }));
    }

    return [];
  }

  public render() {
    // const currentHeaderLinks = this.getHeaderLinks(this.props.children as React.ReactElement<any>);

    return (
      <BaseLayout title={`Dataform docs | ${this.props.current.attributes.title}`}>
        <div className={styles.container}>
          <div className={styles.sidebar}>
            <Navigation
              currentPath={this.props.current.file.path}
              version={this.props.version}
              tree={this.props.index}
            />
          </div>
          <div className={styles.mainContent}>
            <div className={styles.titleBlock}>
              <h1>{this.props.current.attributes.title}</h1>
              <div className={styles.titleRight}>
                <a
                  href={`https://github.com/dataform-co/dataform/blob/master/content/docs/${this.props.current.file.path}.md`}
                >
                  <Button icon="edit" text="Suggest edits" minimal={true} />
                </a>
              </div>
            </div>
            {this.props.children}
          </div>
        </div>
      </BaseLayout>
    );
  }
}
