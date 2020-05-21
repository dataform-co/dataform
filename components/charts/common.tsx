import * as React from "react";

export interface IChartProps {
  horizontal?: boolean;
  formatCountValue?: (value: React.ReactText) => string;
  maxLabelLength?: number;
  dateFormat?: boolean;
}

interface IChartPadding {
  categoryAxisPadding: number;
  countAxisPadding: number;
  longestCategoryAxisValue: number;
}

export function generateAxis({
  maxLabelLength,
  horizontal,
  categoryAxisValues,
  formatCountValue,
  dateFormat
}: IChartProps & { categoryAxisValues: string[] }) {
  // TODO: Update the nivo.rocks library to calculate spacing for axis properly.

  const longestCategoryAxisValue = Math.min(
    categoryAxisValues
      .filter(val => !!val)
      .reduce((acc, val) => (acc > val.length ? acc : val.length), 0),
    maxLabelLength
  );

  const categoryAxisPadding =
    longestCategoryAxisValue < 12 || dateFormat
      ? 0
      : horizontal
      ? longestCategoryAxisValue * 5
      : longestCategoryAxisValue * 2;
  const countAxisPadding =
    longestCategoryAxisValue < 12 || dateFormat ? 0 : horizontal ? 0 : longestCategoryAxisValue * 2;

  const categoryAxis = {
    tickRotation: !!horizontal ? 0 : -30,
    format: dateFormat
      ? "%b %d"
      : (value: React.ReactText) => formatStringValue(value, longestCategoryAxisValue)
  };
  const countAxis = {
    format: (value: React.ReactText) => formatCountValue(value)
  };

  return { categoryAxisPadding, countAxisPadding, categoryAxis, countAxis };
}

export function formatStringValue(value: React.ReactText, maxLength = 14): string {
  const stringValue = String(value);
  return stringValue.length > maxLength
    ? `${stringValue.substring(0, maxLength - 3)}...`
    : stringValue;
}

export const nivoTheme = {
  tooltip: {
    container: {
      boxShadow: "var(--popUpShadow)",
      backgroundColor: "var(--shade0)",
      borderRadius: "5px",
      padding: "9px 12px"
    }
  },
  axis: {
    ticks: {
      text: {
        fill: "var(--textPrimary)",
        fontSize: "12px",
        fontFamily: "Open Sans"
      }
    }
  },
  crosshair: {
    line: {
      stroke: "var(--textSecondary)",
      strokeWidth: 2
    }
  }
};
