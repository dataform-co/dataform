import * as React from "react";
import { Menu, MenuItem } from "@blueprintjs/core";

export default class Navigation extends React.Component<any, any> {
  render() {
    return (
      <div>
        <h3>Guides</h3>
        <Menu style={styles.menu}>
          <MenuItem href="/guides/core-concepts" text="Core concepts" />
          <MenuItem href="/guides/materializations" text="Publishing tables" />
          <MenuItem href="/guides/incremental-tables" text="Building incremental tables" />
          <MenuItem href="/guides/includes" text="Re-usable code with includes" />
          <MenuItem href="/guides/operations" text="Custom SQL operations" />
          <MenuItem href="/guides/assertions" text="Testing data with assertions" />
          <MenuItem href="/guides/configuration" text="Project configuration" />
          <MenuItem href="/guides/js-api" text="JavaScript API" />
          {/*<MenuItem href="/guides/command-line-interface" text="Command line interface" />*/}
          <MenuItem text="Warehouse integrations" />
          <div style={styles.indent1}>
            <MenuItem href="/guides/warehouses/bigquery" text="BigQuery" />
            <MenuItem href="/guides/warehouses/redshift" text="Redshift" />
          </div>
        </Menu>
        <h3>Reference</h3>
        <Menu style={styles.menu}>
          <MenuItem href="/reference/assertions" text="Assertions" />
          <MenuItem href="/reference/contextable" text="Contextable<> Class" />
          <MenuItem href="/reference/dataform-json" text="dataform.json" />
          <MenuItem href="/reference/js-api" text="JS API" />
          <MenuItem href="/reference/materialization-config" text="MaterializationConfig Class" />
          <MenuItem href="/reference/operations" text="Operations" />
          <MenuItem href="/reference/materializations" text="Materializations" />
        </Menu>
      </div>
    );
  }
}

export const styles: { [className: string]: React.CSSProperties } = {
  menu: {
    backgroundColor: "transparent"
  },
  indent1: {
    marginLeft: "16px"
  }
};
