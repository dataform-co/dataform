import { Overview } from "@dataform/app/overview";
import { Service } from "@dataform/app/service";
import * as React from "react";
import * as ReactDOM from "react-dom";
import { Route } from "react-router";
import { BrowserRouter } from "react-router-dom";

const service = Service.get();

async function render() {
  const metadata = await service.metadata({});
  ReactDOM.render(
    <BrowserRouter>
      <Route
        path={"/"}
        exact={true}
        component={(props: any) => <Overview {...props} service={service} metadata={metadata} />}
      />
    </BrowserRouter>,
    document.getElementById("root")
  );
}

const _ = render();
