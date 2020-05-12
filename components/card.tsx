import { Alignment, H3 } from "@blueprintjs/core";
import * as styles from "df/components/card.css";
import * as React from "react";

export interface ICardProps {
  header?: React.ReactNode;
  headerRight?: React.ReactNode;
  minimal?: boolean;
  fullWidth?: boolean;
}

export const Card = ({
  header,
  headerRight,
  children,
  className,
  minimal,
  fullWidth,
  ...rest
}: React.PropsWithChildren<ICardProps> & React.HTMLAttributes<HTMLDivElement>) => (
  <div
    {...rest}
    className={[
      className,
      styles.cardContainer,
      minimal ? styles.minimal : "",
      !header && !headerRight ? styles.headerless : ""
    ].join(" ")}
  >
    <div className={styles.cardMainContainer}>
      {(header || headerRight) && (
        <span className={styles.cardHeaderContainer}>
          <H3>{header}</H3>
          {headerRight && (
            <span className={styles.cardSecondaryActionsContainer}>{headerRight}</span>
          )}
        </span>
      )}
      <div
        className={`${styles.cardContentContainer} ${
          fullWidth ? styles.fullWithCardContentContainer : ""
        }`}
      >
        {children}
      </div>
    </div>
  </div>
);

export interface ICardActionsProps {
  align?: Alignment;
}

export const CardActions = ({
  children,
  align = "left",
  className,
  ...rest
}: React.PropsWithChildren<ICardActionsProps> & React.HTMLAttributes<HTMLDivElement>) => (
  <>
    <div className={styles.cardActionsSpacer} />
    <div {...rest} className={[styles.cardActions, styles[align], className].join(" ")}>
      {children}
    </div>
  </>
);

export const CardMedia = ({
  children,
  className,
  ...rest
}: React.PropsWithChildren<{}> & React.HTMLAttributes<HTMLDivElement>) => (
  <div {...rest} className={[styles.cardMedia, className].join(" ")}>
    {children}
  </div>
);

export interface ICardGridProps {
  minWidth?: number;
}

export const CardGrid = ({
  minWidth = 400,
  children,
  className,
  ...rest
}: ICardGridProps & React.PropsWithChildren<{}> & React.HTMLAttributes<HTMLDivElement>) => {
  return (
    <div
      className={[styles.cardGrid, className].join(" ")}
      style={{ gridTemplateColumns: `repeat(auto-fit, minmax(${minWidth}px, 1fr))` }}
      {...rest}
    >
      {children}
    </div>
  );
};

export const CardList = ({
  className,
  children,
  ...rest
}: React.PropsWithChildren<{}> & React.HTMLAttributes<HTMLDivElement>) => {
  return (
    <div className={[styles.cardList, className].join(" ")} {...rest}>
      {children}
    </div>
  );
};
