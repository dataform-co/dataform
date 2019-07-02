import { Menu, MenuItem as BPMenuItem } from "@blueprintjs/core";
import { pathFromFilename } from "df/docs/common/paths";
import * as styles from "df/docs/components/navigation.css";
import * as React from "react";

// Framework guides.
import * as guideAssertions from "df/docs/pages/guides/assertions.mdx";
import * as guideCLI from "df/docs/pages/guides/command-line-interface.mdx";
import * as guideConfiguration from "df/docs/pages/guides/configuration.mdx";
import * as guideCoreConcepts from "df/docs/pages/guides/core-concepts.mdx";
import * as guideDatasets from "df/docs/pages/guides/datasets.mdx";
import * as guideIncludes from "df/docs/pages/guides/includes.mdx";
import * as guideIncrementalDatasets from "df/docs/pages/guides/incremental-datasets.mdx";
import * as guideBuiltInFunctions from "df/docs/pages/guides/built-in-functions.mdx";
import * as guideJsApi from "df/docs/pages/guides/js-api.mdx";
import * as guideOperations from "df/docs/pages/guides/operations.mdx";

// Framework warehouses.
import * as guideBigQuery from "df/docs/pages/guides/warehouses/bigquery.mdx";
import * as guideRedshift from "df/docs/pages/guides/warehouses/redshift.mdx";

// Platform.
import * as platformPublishTables from "df/docs/pages/platform_guides/publish_tables.mdx";
import * as platformScheduling from "df/docs/pages/platform_guides/scheduling.mdx";
import * as platformSetupDataWarehouse from "df/docs/pages/platform_guides/set_up_datawarehouse.mdx";
import * as platformVersionControl from "df/docs/pages/platform_guides/version_control.mdx";

interface IProps {
  currentPath: string;
  currentHeaderLinks: IHeaderLink[];
}

export interface IHeaderLink {
  id: string;
  text: string;
}

export default class Navigation extends React.Component<IProps> {
  public render() {
    return (
      <div className={styles.navigation}>
        <h4>Framework</h4>
        <Menu className={styles.menu}>
          {[
            guideCoreConcepts,
            guideDatasets,
            guideIncrementalDatasets,
            guideIncludes,
            guideOperations,
            guideAssertions,
            guideConfiguration,
            guideBuiltInFunctions,
            guideJsApi,
            guideCLI
          ].map(page => this.menuItem(page.meta))}
          <div className={styles.subtitle}>Warehouse integrations</div>
          {[guideBigQuery, guideRedshift].map(page => this.menuItem(page.meta))}
        </Menu>
        <h4>Web</h4>
        <Menu className={styles.menu}>
          {[
            platformSetupDataWarehouse,
            platformPublishTables,
            platformVersionControl,
            platformScheduling
          ].map(page => this.menuItem(page.meta))}
        </Menu>
      </div>
    );
  }
  private menuItem = meta => {
    const path = pathFromFilename(meta.__filename);
    if (this.props.currentPath === path) {
      return (
        <React.Fragment key={path}>
          <BPMenuItem
            href={path}
            text={meta.title}
            style={{ backgroundColor: "rgba(167, 182, 194, 0.3)" }}
          />
          <div className={styles.indent1}>
            {this.props.currentHeaderLinks.map(item => (
              <BPMenuItem href={`#${item.id}`} text={item.text} key={item.id} />
            ))}
          </div>
        </React.Fragment>
      );
    }
    return <BPMenuItem key={path} href={path} text={meta.title} />;
  };
}
