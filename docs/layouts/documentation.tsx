import Method from "df/docs/components/method";
import Navigation, { IHeaderLink } from "df/docs/components/navigation";
import * as React from "react";

import { pathFromFilename } from "df/docs/common/paths";
import { Search } from "df/docs/components/search";
import { BaseLayout } from "df/docs/layouts/base";
import * as styles from "df/docs/layouts/documentation.css";

export interface IProps {
  title: string;
  __filename: string;
}

export default class Documentation extends React.Component<IProps> {
  public getHeaderLinks(child: React.ReactElement<any>): IHeaderLink[] {
    if (child && child.props && Array.isArray(child.props.children)) {
      const headers = child.props.children
        .filter(item => item.props.name === "h2")
        .map(item => ({
          id: item.props.props.id,
          text: item.props.children
        }));

      const methods = child.props.children
        .filter(item => item.type === Method)
        .map(item => ({
          id: item.props.name,
          text: item.props.name
        }));

      return [...headers, ...methods];
    }

    return [];
  }

  public render() {
    const currentHeaderLinks = this.getHeaderLinks(this.props.children as React.ReactElement<any>);
    return (
      <BaseLayout title={`Dataform docs | ${this.props.title}`}>
        <div className={styles.container}>
          <div className={styles.sidebar}>
            <Search />
            <Navigation
              currentPath={pathFromFilename(this.props.__filename)}
              currentHeaderLinks={currentHeaderLinks}
            />
          </div>
          <div className={styles.mainContent}>
            <h1>{this.props.title}</h1>
            {this.props.children}
          </div>
        </div>
      </BaseLayout>
    );
  }
}
