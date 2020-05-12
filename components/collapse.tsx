import { Button, Collapse } from "@blueprintjs/core";
import { useState } from "react";
import * as React from "react";

import * as styles from "df/components/collapse.css";

interface ISimpleCollapseProps {
  message: React.ReactNode;
}

export function SimpleCollapse({
  message,
  children
}: React.PropsWithChildren<ISimpleCollapseProps>) {
  const [show, updateShow] = useState(false);
  return (
    <>
      <div className={styles.messageContainer}>
        {message}
        {children && (
          <Button
            minimal={true}
            className={styles.button}
            onClick={() => updateShow(!show)}
            icon={show ? "caret-up" : "caret-down"}
          />
        )}
      </div>
      {children && <Collapse isOpen={show}>{children}</Collapse>}
    </>
  );
}
