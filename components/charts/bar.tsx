import { BarSvgProps, ResponsiveBar } from "@nivo/bar";
import * as styles from "df/components/charts/bar.css";
import { IChartProps, nivoTheme } from "df/components/charts/common";
import { generateAxis } from "df/components/charts/common";
import * as React from "react";

// Required as Nivo bar data is just `object[]`.
type BarData = Array<{
  [x: string]: string | number;
  id: string;
}>;

export const Bar = (props: BarSvgProps & IChartProps) => {
  const { data, horizontal, formatCountValue, margin } = props;
  const categoryAxisValues = (data as BarData).map(datum => datum.id);
  const { categoryAxisPadding, countAxisPadding, categoryAxis, countAxis } = generateAxis({
    ...props,
    categoryAxisValues
  });

  return (
    <div
      className={styles.chartContent}
      style={{
        paddingLeft: `${horizontal ? categoryAxisPadding : countAxisPadding}px`,
        paddingBottom: `${horizontal ? countAxisPadding : categoryAxisPadding}px`
      }}
    >
      <ResponsiveBar
        data={data}
        animate={false}
        margin={{
          top: 10,
          right: 10,
          bottom: 60,
          left: 60,
          ...margin
        }}
        theme={nivoTheme}
        axisLeft={horizontal ? categoryAxis : countAxis}
        axisBottom={horizontal ? countAxis : categoryAxis}
        tooltip={({ id, value, color }) => (
          <div>
            <div>{id}</div>
            <div>
              <strong style={{ borderBottom: `3px solid ${color}` }}>
                {formatCountValue(value)}
              </strong>
            </div>
          </div>
        )}
        {...props}
      />
    </div>
  );
};
