publish("jit_regional_summary", {
  type: "view",
  description: "JiT-powered regional summary showing dynamic grouping logic."
}).jitCode(async (ctx) => {
    // Branch grouping logic based on variables
    const groupByMonth = ctx.projectConfig.vars.groupByMonth === "true";
    const selectDate = groupByMonth 
        ? "DATE_TRUNC(DATE(year, month, day), MONTH) AS period" 
        : "DATE(year, month, day) AS period";

    return `
        SELECT
            ${selectDate},
            station_number,
            AVG(mean_temp) AS avg_temp
        FROM
            \`bigquery-public-data.samples.gsod\`
        WHERE
            year >= 2010
        GROUP BY
            1, 2
        ORDER BY
            period DESC, avg_temp DESC
        LIMIT 100
    `;
});
