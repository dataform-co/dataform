import { Button, Navbar, NavbarGroup, Tag } from "@blueprintjs/core";
import * as styles from "df/app/overview.css";
import { Service } from "df/app/service";
import { dataform } from "df/protos/ts";
import * as React from "react";

interface IProps {
  service: Service;
  metadata: dataform.server.MetadataResponse;
}

export class Overview extends React.Component<IProps> {
  public render() {
    return (
      <div className={styles.overviewContainer}>
        <Navbar>
          <NavbarGroup>
            <img src="/public/new_logo_with_text.svg" />
          </NavbarGroup>
          <NavbarGroup align="right">
            <Tag>{this.props.metadata.projectDir}</Tag>
          </NavbarGroup>
        </Navbar>
      </div>
    );
  }
}
