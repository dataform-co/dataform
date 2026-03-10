publish("jit_weather_analysis", {
    type: "view",
    description: "JiT-powered weather analysis demonstrating dynamic structure and math."
}).jitCode(async (ctx) => {
    const table = "`bigquery-public-data.samples.gsod`";
    
    // Programmatically select columns based on requested metrics from vars
    const metricsStr = ctx.projectConfig.vars.metrics || "snow,thunder,tornado";
    const metrics = metricsStr.split(",");
    const metricColumns = metrics.map(m => `SUM(CAST(${m} AS INT64)) AS ${m}_days_count`).join(",\n            ");
    const metricsFilter = metrics.map(m => `${m} = TRUE`).join(" OR ");

    // Inject different math formulas and thresholds based on unit preference
    const tempUnit = ctx.projectConfig.vars.tempUnit || "F"; // F or C
    const threshold = parseFloat(ctx.projectConfig.vars.tempThreshold) || 80.0;
    
    const tempExpr = tempUnit === "C" 
        ? "((max_temp - 32) * 5 / 9)" 
        : "max_temp";
    
    const limit = ctx.projectConfig.vars.environment === "test" ? 10 : 1000;

    return `
        SELECT
            DATE_TRUNC(DATE(year, month, day), MONTH) AS date,
            ${metricColumns},
            AVG(${tempExpr}) AS avg_max_temp_${tempUnit},
            COUNTIF(${tempExpr} > ${threshold}) AS days_above_threshold
        FROM
            ${table}
        WHERE
            (${metricsFilter})
            AND year >= 2007
        GROUP BY
            date
        ORDER BY
            date
        LIMIT
            ${limit}
    `;
});
