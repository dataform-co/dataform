import { Alignment, H3 } from "@blueprintjs/core";
import * as React from "react";

import { BreadCrumb, IBreadCrumbProps } from "df/components/breadcrumb";
import * as styles from "df/components/card.css";

export interface ICardProps {
  header?: React.ReactNode;
  headerRight?: React.ReactNode;
  minimal?: boolean;
  fullWidth?: boolean;
  masonryCard?: boolean;
  breadCrumbs?: IBreadCrumbProps;
}

export const Card = ({
  header,
  headerRight,
  children,
  className,
  minimal,
  fullWidth,
  masonryCard,
  breadCrumbs,
  ...rest
}: React.PropsWithChildren<ICardProps> & React.HTMLAttributes<HTMLDivElement>) => (
  <div
    {...rest}
    className={[
      className,
      styles.cardContainer,
      minimal ? styles.minimal : "",
      !header && !headerRight ? styles.headerless : "",
      masonryCard ? styles.masonryCardContainer : styles.flexCardContainer
    ].join(" ")}
  >
    <div className={styles.cardMainContainer}>
      {breadCrumbs && (
        <span className={styles.cardHeaderContainer}>
          <BreadCrumb {...breadCrumbs} />
        </span>
      )}
      {(header || headerRight) && (
        <span className={styles.cardHeaderContainer}>
          {header && <H3>{header}</H3>}
          {headerRight && (
            <span className={styles.cardSecondaryActionsContainer}>{headerRight}</span>
          )}
        </span>
      )}
      <div
        className={`${styles.cardContentContainer} ${
          fullWidth ? styles.fullWidthCardContentContainer : ""
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

export const CardMasonry = ({
  children,
  className,
  ...rest
}: ICardGridProps & React.PropsWithChildren<{}> & React.HTMLAttributes<HTMLDivElement>) => {
  return (
    <div className={[styles.cardMasonry, className].join(" ")} {...rest}>
      {children}
    </div>
  );
};
