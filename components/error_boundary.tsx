import { Button, Callout, Card, Collapse, H4, Intent } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/components/error_boundary.css";

export interface IBaseErrorProps {
  customTitle?: React.ReactNode | string;
  customIntent?: Intent;
  sinkMethod?: (error: Error, id: string) => void;
}

export interface IErrorCalloutProps extends IBaseErrorProps {
  errorMessage?: string;
}

interface IBaseErrorState {
  showErrorDetails: boolean;
}

export class ErrorCallout extends React.Component<IErrorCalloutProps, IBaseErrorState> {
  public state: IBaseErrorState = {
    showErrorDetails: false
  };

  public toggleShowErrorDetails = () => {
    this.setState({ showErrorDetails: !this.state.showErrorDetails });
  };

  public render() {
    const { showErrorDetails } = this.state;
    const {
      customTitle = "Something went wrong. Try refreshing the page or",
      customIntent = Intent.DANGER,
      errorMessage
    } = this.props;
    return (
      <Callout intent={customIntent}>
        <H4 className={styles.errorTitleContainer}>
          <span>
            {customTitle}
            {!errorMessage && <br />}
            <a
              className={styles.contactUsLink}
              onClick={() => (window as any).Intercom && (window as any).Intercom("show")}
            >
              {" "}
              contact us for help
            </a>
            .
          </span>

          {!!errorMessage && (
            <Button
              className={styles.errorButton}
              icon={showErrorDetails ? "caret-up" : "caret-down"}
              onClick={this.toggleShowErrorDetails}
              minimal={true}
            />
          )}
        </H4>
        <Collapse isOpen={showErrorDetails}>
          <pre>{errorMessage}</pre>
        </Collapse>
      </Callout>
    );
  }
}

export class ErrorPageWithCallout extends React.Component<IErrorCalloutProps, IBaseErrorState> {
  public render() {
    return (
      <div className={styles.errorPageContainer}>
        <Card className={`${styles.content_centered} ${styles.errorPageCard}`}>
          <ErrorCallout {...this.props} />
        </Card>
      </div>
    );
  }
}

const ErrorBoundaryComponent = (props: IErrorCalloutProps) => (
  <div className={styles.errorComponentWrapper}>
    <ErrorCallout {...props} />
  </div>
);

const ErrorBoundaryPage = (props: IErrorCalloutProps) => (
  <div className={styles.errorPageContainer}>
    <Card className={`${styles.content_centered} ${styles.errorPageCard}`}>
      <ErrorCallout {...props} />
    </Card>
  </div>
);

export interface IErrorBoundaryState {
  errorMessage?: string;
}

export interface IErrorBoundaryProps extends IBaseErrorProps {
  type: "page" | "component";
  id: string;
}

export class ErrorBoundary extends React.Component<IErrorBoundaryProps, IErrorBoundaryState> {
  public static getDerivedStateFromError(error: Error) {
    return { errorMessage: error.toString() };
  }

  public state: IErrorBoundaryState = {
    errorMessage: ""
  };

  public componentDidCatch(error: Error) {
    if (!!this.props.sinkMethod) {
      this.props.sinkMethod(error, this.props.id);
    }
  }

  public render() {
    const { errorMessage } = this.state;
    if (errorMessage) {
      return (
        <>
          {this.props.type === "page" && (
            <ErrorBoundaryPage {...this.props} errorMessage={errorMessage} />
          )}
          {this.props.type === "component" && (
            <ErrorBoundaryComponent {...this.props} errorMessage={errorMessage} />
          )}
        </>
      );
    }

    return this.props.children;
  }
}
