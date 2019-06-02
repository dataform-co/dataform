import { Menu, MenuDivider, MenuItem as BPMenuItem } from "@blueprintjs/core";
import * as styles from "df/docs/components/navigation.css";
import * as React from "react";

interface IProps {
  currentPath: string;
  onThisPageItems: IHeaderLink[];
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
          <this.MenuItem href="/guides/core-concepts" text="Core concepts" />
          <this.MenuItem href="/guides/datasets" text="Publishing datasets" />
          <this.MenuItem href="/guides/incremental-datasets" text="Building incremental datasets" />
          <this.MenuItem href="/guides/includes" text="Re-usable code with includes" />
          <this.MenuItem href="/guides/operations" text="Custom SQL operations" />
          <this.MenuItem href="/guides/assertions" text="Testing data with assertions" />
          <this.MenuItem href="/guides/configuration" text="Project configuration" />
          <this.MenuItem href="/guides/js-api" text="JavaScript API" />
          <this.MenuItem href="/guides/command-line-interface" text="Command line interface" />
          <div className={styles.subtitle}>Warehouse integrations</div>
          <this.MenuItem href="/guides/warehouses/bigquery" text="BigQuery" />
          <this.MenuItem href="/guides/warehouses/redshift" text="Redshift" />
        </Menu>
        <h4>Web</h4>
        <Menu className={styles.menu}>
          <this.MenuItem
            href="/platform_guides/set_up_datawarehouse"
            text="Set up your cloud data warehouse"
          />
          <this.MenuItem
            href="/platform_guides/publish_tables"
            text="Publish your first datasets"
          />
          <this.MenuItem href="/platform_guides/version_control" text="Use version control" />
          <this.MenuItem href="/platform_guides/scheduling" text="Schedule runs" />
        </Menu>
        <h4>API reference</h4>
        <Menu className={styles.menu}>
          <this.MenuItem href="/reference/assertions" text="Assertions" />
          {/* <this.MenuItem href="/reference/contextable" text="Contextable<> Class" />*/}
          <this.MenuItem href="/reference/dataform-json" text="dataform.json" />
          <this.MenuItem href="/reference/js-api" text="JS API" />
          {/* <this.MenuItem href="/reference/table-config" text="TableConfig Class" /> */}
          <this.MenuItem href="/reference/operations" text="Operations" />
          <this.MenuItem href="/reference/datasets" text="Datasets" />
        </Menu>
      </div>
    );
  }
  private MenuItem = props => {
    if (this.props.currentPath === props.href) {
      return (
        <React.Fragment>
          <BPMenuItem {...props} style={{ backgroundColor: "rgba(167, 182, 194, 0.3);" }} />
          <div className={styles.indent1}>
            {this.props.onThisPageItems.map(item => (
              <BPMenuItem href={`#${item.id}`} text={item.text} key={item.id} />
            ))}
          </div>
        </React.Fragment>
      );
    }
    return <BPMenuItem {...props} />;
  };
}
