import { FormGroup, Intent } from "@blueprintjs/core";
import * as React from "react";

import { ControlledDialog, IProps as BaseProps } from "df/components/controlled_dialog";

interface IProps extends BaseProps {
  labelText?: string;
  labelHelperText?: string;
  onDelete: () => void;
}

export function DeleteDialog(props: React.PropsWithChildren<IProps>) {
  return (
    <ControlledDialog
      confirmButtonProps={{ text: "Delete", intent: Intent.DANGER }}
      onConfirm={props.onDelete}
      {...props}
    >
      {props.labelText && <FormGroup label={props.labelText} helperText={props.labelHelperText} />}
      {props.children}
    </ControlledDialog>
  );
}
