import Method from "df/docs/components/method";
import Navigation from "df/docs/components/navigation";
import OnThisPage from "df/docs/components/on_this_page";
import * as React from "react";
import Media from "react-media";

import { BaseLayout } from "df/docs/layouts/base";

import * as commonStyles from "df/docs/common.css";
import * as styles from "df/docs/layouts/documentation.css";

export interface Props {
  title: string;
}

export default class Documentation extends React.Component<Props, any> {
  public getMenuItems = (child: React.ReactElement<any>) => {
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
  };

  public renderRightSidebar = () => {
    const menu = this.getMenuItems(this.props.children as React.ReactElement<any>);

    return (
      <div className={styles.sidebar}>
        <OnThisPage menu={menu} />
      </div>
    );
  };

  public render() {
    return (
      <BaseLayout title="Dataform | Documentation">
        <div className={commonStyles.flexRow}>
          <div className={styles.sidebar}>
            <Navigation />
          </div>
          <div className={styles.mainContent}>
            <h1>{this.props.title}</h1>
            {this.props.children}
          </div>
          <Media query="(min-width: 1200px)" render={this.renderRightSidebar} />
        </div>
      </BaseLayout>
    );
  }
}
