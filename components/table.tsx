import { HTMLTable } from "@blueprintjs/core";
import * as styles from "df/components/table.css";
import * as React from "react";

export interface IRow {
  cells: Array<React.ReactElement | string>;
  colspans?: number[];
  href?: string;
  onClick?: () => void;
  onMouseEnter?: (event: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => void;
  onMouseLeave?: (event: React.MouseEvent<HTMLAnchorElement, MouseEvent>) => void;
}

export interface ITableProps {
  headers: Array<React.ReactElement | string>;
  rows: IRow[];
  className?: string;
  condensed?: boolean;
  columnWidths?: number[];
}

export const Row = ({
  colspans,
  ...rest
}: {
  children: React.ReactElement;
} & IRow) => {
  if (rest.href || rest.onClick) {
    return (
      <a className={styles.tableRowLink} {...rest}>
        {rest.children}
      </a>
    );
  }

  return <tr>{rest.children}</tr>;
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
      {rows.map((row, rowIndex) => (
        <Row key={rowIndex} {...row}>
          <>
            {row.cells.map((cell, cellIndex) => (
              <td
                colSpan={row.colspans && row.colspans[cellIndex]}
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
