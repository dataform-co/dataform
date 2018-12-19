import * as React from "react";
import { Menu, MenuItem } from "@blueprintjs/core";

export default class Navigation extends React.Component<any, any> {
  render() {
    return (
      <div>
        <h3>Guides</h3>
        <Menu style={styles.menu}>
          <MenuItem href="/guides/assertions" text="Assertions" />
          <MenuItem href="/guides/configuration" text="Configuration" />
          <MenuItem href="/guides/core-concepts" text="Core concepts" />
          <MenuItem href="/guides/includes" text="Includes" />
          <MenuItem href="/guides/js-api" text="JS API" />
          <MenuItem href="/guides/operations" text="Operations" />
          <MenuItem href="/guides/materializations" text="Materializations" />
          <MenuItem href="/guides/command-line-interface" text="Command line interface" />
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
  }
};
