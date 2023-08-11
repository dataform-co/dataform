import * as React from "react";
import { browserHistory, Route, Router } from "react-router";

import { Overview } from "df/app/overview";
import { Service } from "df/app/service";
import * as ReactDOM from "react-dom";

const service = Service.get();

async function render() {
  const metadata = await service.metadata({});
  ReactDOM.render(
    <Router history={browserHistory}>
      <Route
        path={"/"}
        component={(props: any) => <Overview {...props} service={service} metadata={metadata} />}
      />
    </Router>,
    document.getElementById("root")
  );
}

const _ = render();
