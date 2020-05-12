import { Dialog as BlueprintDialog, IDialogProps } from "@blueprintjs/core";
import { Card } from "df/components/card";
import * as styles from "df/components/dialog.css";
import * as React from "react";

export const Dialog = ({
  title,
  className,
  children,
  ...rest
}: React.PropsWithChildren<IDialogProps>) => {
  return (
    <BlueprintDialog
      className={styles.dialog}
      isCloseButtonShown={false}
      canOutsideClickClose={true}
      hasBackdrop={true}
      usePortal={true}
      autoFocus={true}
      {...rest}
    >
      <Card className={styles.dialogCard} header={title}>
        {children}
      </Card>
    </BlueprintDialog>
  );
};
