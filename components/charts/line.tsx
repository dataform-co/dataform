import { LineSvgProps, ResponsiveLine } from "@nivo/line";
import { Serie } from "@nivo/line";
import * as React from "react";

import { IChartProps, nivoTheme } from "df/components/charts/common";
import { generateAxis } from "df/components/charts/common";
import * as styles from "df/components/charts/line.css";

export const Line = (props: LineSvgProps & IChartProps) => {
  const { data, horizontal, formatCountValue, margin } = props;
  const categoryAxisValues = data.map(datum => datum.id.toString());
  const { categoryAxisPadding, countAxisPadding, categoryAxis, countAxis } = generateAxis({
    categoryAxisValues,
    ...props
  });

  return (
    <div
      className={styles.chartContent}
      style={{
        paddingLeft: `${horizontal ? categoryAxisPadding : countAxisPadding}px`,
        paddingBottom: `${horizontal ? countAxisPadding : categoryAxisPadding}px`
      }}
    >
      <ResponsiveLine
        data={data}
        animate={false}
        enableGridX={false}
        isInteractive={true}
        enableCrosshair={true}
        enableSlices={"x"}
        useMesh={true}
        margin={{
          top: 10,
          right: 10,
          bottom: 60,
          left: 60,
          ...margin
        }}
        axisLeft={horizontal ? categoryAxis : countAxis}
        axisBottom={horizontal ? countAxis : categoryAxis}
        sliceTooltip={({ slice }) => {
          const date = slice.points[0].data.xFormatted;
          return (
            <div className={styles.tooltip}>
              {slice.points.map((point, index) => (
                <>
                  <div key={`${date}${point.id}`}>
                    {point.serieId !== "default" ? `${point.serieId}, ${date}` : date}
                  </div>
                  <div
                    key={point.id}
                    style={{
                      marginBottom: index < slice.points.length - 1 ? "12px" : "4px"
                    }}
                  >
                    <strong style={{ borderBottom: `3px solid ${point.serieColor}` }}>
                      {formatCountValue(point.data.yFormatted)}
                    </strong>
                  </div>
                </>
              ))}
            </div>
          );
        }}
        pointSize={8}
        pointBorderWidth={2}
        pointColor="var(--shade1)"
        pointBorderColor={{ from: "serieColor" }}
        theme={nivoTheme}
        {...props}
      />
    </div>
  );
};
