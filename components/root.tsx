import * as React from "react";

// Inject all the blueprint stylesheets.
// The following order is important!
// tslint:disable: no-var-requires
require("df/components/blueprint/buttons.global.css");
require("df/components/blueprint/callout.global.css");
require("df/components/blueprint/card.global.css");
require("df/components/blueprint/dialog.global.css");
require("df/components/blueprint/drawer.global.css");
require("df/components/blueprint/navbar.global.css");
require("df/components/blueprint/popover.global.css");
require("df/components/blueprint/toast.global.css");
require("df/components/blueprint/tooltip.global.css");
require("df/components/blueprint/core.global.css");
require("df/components/blueprint/tag.global.css");
require("df/components/blueprint/icons.global.css");
require("df/components/blueprint/datetime.global.css");
require("df/components/blueprint/table.global.css");
require("df/components/blueprint/select.global.css");
require("df/components/root.css");
require("df/components/blueprint/custom.global.css");
// tslint:enable

export const Root = ({
  className,
  children,
  ...rest
}: React.PropsWithChildren<{}> & React.HTMLAttributes<HTMLDivElement>) => (
  <div {...rest}>{children}</div>
);
