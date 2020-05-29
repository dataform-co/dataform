import * as React from "react";

import { Button, IButtonProps, Intent } from "@blueprintjs/core";
import { CardActions } from "df/components/card";
import { Dialog } from "df/components/dialog";

export interface IProps {
  title?: string;
  ref?: React.RefObject<ControlledDialog>;
  button?: React.ReactElement<any>;
  onConfirm?: () => void;
  onClose?: (e: any) => void;
  confirmButtonProps?: IButtonProps & React.ButtonHTMLAttributes<HTMLButtonElement>;
  cancelButtonText?: string;
  confirmHidden?: boolean;
  ignore?: boolean;
  width?: string;
}

interface IState {
  isOpen: boolean;
}

export class ControlledDialog extends React.Component<IProps, IState> {
  constructor(props: IProps) {
    super(props);
    this.state = { isOpen: false };
  }

  public showDialog() {
    this.setState({ isOpen: true });
  }

  public render() {
    return (
      <>
        <span
          onClick={e => {
            this.setState({ isOpen: this.props.ignore ? false : true });
            e.stopPropagation();
          }}
        >
          {this.props.button}
        </span>
        <Dialog
          isOpen={this.state.isOpen}
          title={this.props.title}
          onClose={e => {
            this.setState({ isOpen: false });
            if (!!this.props.onClose) {
              this.props.onClose(e);
            }
          }}
          hasBackdrop={true}
          usePortal={true}
          autoFocus={true}
          style={{ width: this.props.width || "600px" }}
        >
          <div
            onKeyDown={e => {
              if (e.key === "Enter" && !this.props.confirmButtonProps?.disabled) {
                e.preventDefault();
                this.props.onConfirm();
                this.setState({ isOpen: false });
              }
            }}
          >
            {this.props.children}
          </div>
          <CardActions align="right">
            <Button
              minimal={true}
              onClick={(e: React.MouseEvent<HTMLElement, MouseEvent>) => {
                this.setState({ isOpen: false });
                if (!!this.props.onClose) {
                  this.props.onClose(e);
                }
              }}
            >
              {this.props.cancelButtonText || "Cancel"}
            </Button>
            {!this.props.confirmHidden && (
              <Button
                {...this.props.confirmButtonProps}
                name="confirm"
                intent={this.props.confirmButtonProps?.intent || Intent.PRIMARY}
                onClick={() => {
                  this.props?.onConfirm();
                  this.setState({ isOpen: false });
                }}
                text={this.props.confirmButtonProps?.text || "Confirm"}
              />
            )}
          </CardActions>
        </Dialog>
      </>
    );
  }
}
