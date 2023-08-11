import { Button, Classes, IButtonProps, Intent } from "@blueprintjs/core";
import * as React from "react";

import { CardActions } from "df/components/card";
import { Dialog } from "df/components/dialog";

export interface IControlledDialogProps {
  title?: string;
  ref?: React.RefObject<ControlledDialog>;
  openButton?: React.ReactElement<Element>;
  onConfirm?: () => void;
  onClose?: (e: any) => void;
  cancelButtonText?: string;
  confirmButtonProps?: IButtonProps & React.ButtonHTMLAttributes<HTMLButtonElement>;
  confirmHidden?: boolean;
  disable?: boolean;
  width?: string;
}

interface IState {
  isOpen: boolean;
}

export class ControlledDialog extends React.Component<IControlledDialogProps, IState> {
  constructor(props: IControlledDialogProps) {
    super(props);
    this.state = { isOpen: false };
  }

  public showDialog() {
    this.setState({ isOpen: true });
  }

  public render() {
    const {
      disable,
      openButton,
      title,
      confirmHidden,
      confirmButtonProps,
      cancelButtonText,
      onClose,
      onConfirm,
      width,
      children
    } = this.props;
    return (
      <>
        <span
          onClick={e => {
            this.setState({ isOpen: disable ? false : true });
            e.stopPropagation();
          }}
        >
          {openButton}
        </span>
        <Dialog
          isOpen={this.state.isOpen}
          title={title}
          onClose={e => {
            this.setState({ isOpen: false });
            if (onClose) {
              onClose(e);
            }
          }}
          hasBackdrop={true}
          usePortal={true}
          autoFocus={true}
          style={{ width: width || "600px" }}
        >
          <div
            onKeyDown={e => {
              if (e.key === "Enter" && !confirmButtonProps?.disabled) {
                e.preventDefault();
                if (onConfirm) {
                  onConfirm();
                }
                this.setState({ isOpen: false });
              }
            }}
          >
            {children}
          </div>
          <CardActions align="right">
            <Button
              minimal={true}
              onClick={(e: React.MouseEvent<HTMLElement, MouseEvent>) => {
                if (onClose) {
                  onClose(e);
                }
                this.setState({ isOpen: false });
              }}
              text={cancelButtonText || "Cancel"}
            />
            {!confirmHidden && (
              <Button
                {...confirmButtonProps}
                name="confirm"
                intent={confirmButtonProps?.intent || Intent.PRIMARY}
                onClick={() => {
                  if (onConfirm) {
                    onConfirm();
                  }
                  this.setState({ isOpen: false });
                }}
                text={confirmButtonProps?.text || "Confirm"}
                className={`${Classes.POPOVER_DISMISS} ${confirmButtonProps?.className || ""}`}
              />
            )}
          </CardActions>
        </Dialog>
      </>
    );
  }
}
