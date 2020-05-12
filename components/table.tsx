import { HTMLTable } from "@blueprintjs/core";
import * as React from "react";

import * as styles from "df/components/table.css";

export type TRow = Array<{
  cells: Array<React.ReactElement | string>;
  colspans?: number[];
  href?: string;
  onClick?: () => void;
}>;

export interface ITableProps {
  headers: Array<React.ReactElement | string>;
  rows: TRow;
  className?: string;
  condensed?: boolean;
  columnWidths?: number[];
}

export const Row = ({
  children,
  href,
  onClick
}: {
  children: React.ReactElement;
  href?: string;
  onClick?: () => void;
}) => {
  if (href) {
    return (
      <a className={styles.tableRowLink} onClick={onClick} href={href}>
        {children}
      </a>
    );
  }

  return <tr>{children}</tr>;
};

export const Table = ({ columnWidths, headers, rows, className, condensed }: ITableProps) => (
  <HTMLTable condensed={condensed} className={`${styles.table} ${className ? className : ""}`}>
    {columnWidths && columnWidths.length > 0 && (
      <colgroup>
        {columnWidths.map((width, index) => (
          <col key={index} span={1} style={{ width: `${width}%` }} />
        ))}
      </colgroup>
    )}
    <thead className={styles.tableHeader}>
      <tr>
        {headers.map((header, headerIndex) => (
          <th key={headerIndex}>{header}</th>
        ))}
      </tr>
    </thead>
    <tbody>
      {rows.map(({ href, cells, colspans }, rowIndex) => (
        <Row key={rowIndex} href={href}>
          <>
            {cells.map((cell, cellIndex) => (
              <td
                colSpan={colspans && colspans[cellIndex]}
                key={cellIndex}
                className={styles.tableCell}
              >
                {cell}
              </td>
            ))}
          </>
        </Row>
      ))}
    </tbody>
  </HTMLTable>
);
