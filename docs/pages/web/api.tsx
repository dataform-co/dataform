import axios from "axios";
import { Swagger } from "df/docs/components/swagger";
import { BaseLayout } from "df/docs/layouts/base";
import * as React from "react";
import { Spec } from "swagger-schema-official";

export interface IProps {
  spec: Spec;
}

export default class Api extends React.Component<IProps> {
  public static async getInitialProps(): Promise<IProps> {
    return {
      spec: (await axios.get("http://localhost:8005/swagger.json")).data
    };
  }

  public render() {
    return (
      <BaseLayout title={"Dataform Web API"}>
        <Swagger spec={this.props.spec} apiHost={"api.dataform.co"}/>
      </BaseLayout>
    );
  }
}
