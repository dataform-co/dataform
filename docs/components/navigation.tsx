import { Menu, MenuDivider, MenuItem } from "@blueprintjs/core";
import * as React from "react";

export default class Navigation extends React.Component<any, any> {
  public render() {
    return (
      <div style={{ paddingTop: "10px" }}>
        <h4>Framework</h4>
        <Menu style={styles.menu}>
          <MenuItem href="/guides/core-concepts" text="Core concepts" />
          <MenuItem href="/guides/tables" text="Publishing tables" />
          <MenuItem href="/guides/incremental-tables" text="Building incremental tables" />
          <MenuItem href="/guides/includes" text="Re-usable code with includes" />
          <MenuItem href="/guides/operations" text="Custom SQL operations" />
          <MenuItem href="/guides/assertions" text="Testing data with assertions" />
          <MenuItem href="/guides/configuration" text="Project configuration" />
          <MenuItem href="/guides/js-api" text="JavaScript API" />
          <MenuItem href="/guides/command-line-interface" text="Command line interface" />
          <MenuDivider title="Warehouse integrations" />
          <div style={styles.indent1}>
            <MenuItem href="/guides/warehouses/bigquery" text="BigQuery" />
            <MenuItem href="/guides/warehouses/redshift" text="Redshift" />
          </div>
        </Menu>
        <h4>Web</h4>
        <Menu style={styles.menu}>
          <MenuItem
            href="/platform_guides/set_up_datawarehouse"
            text="Set up your cloud data warehouse"
          />
          <MenuItem href="/platform_guides/publish_tables" text="Publish your first tables" />
          <MenuItem href="/platform_guides/version_control" text="Use version control" />
          <MenuItem href="/platform_guides/scheduling" text="Schedule runs" />
        </Menu>
        <h4>API reference</h4>
        <Menu style={styles.menu}>
          <MenuItem href="/reference/assertions" text="Assertions" />
          <MenuItem href="/reference/contextable" text="Contextable<> Class" />
          <MenuItem href="/reference/dataform-json" text="dataform.json" />
          <MenuItem href="/reference/js-api" text="JS API" />
          <MenuItem href="/reference/table-config" text="TableConfig Class" />
          <MenuItem href="/reference/operations" text="Operations" />
          <MenuItem href="/reference/tables" text="Tables" />
        </Menu>
      </div>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  menu: {
    backgroundColor: "transparent",
    fontSize: "14px",
    color: "#2f2d44"
  },
  indent1: {
    marginLeft: "16px"
  }
};
