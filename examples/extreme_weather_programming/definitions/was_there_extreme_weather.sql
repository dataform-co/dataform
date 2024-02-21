SELECT
    DATE_TRUNC(DATE(year, month, day), MONTH) AS date,
    COUNT(*) AS extreme_weather_count
FROM
    `bigquery-public-data.samples.gsod`
WHERE
    (
        snow = TRUE
        OR hail = TRUE
        OR thunder = TRUE
        OR tornado = TRUE
    ) -- The GitHub data only ranges from 2007 to 2012.
    -- The weather data is only available up to 2010.
    AND year >= 2007
GROUP BY
    date
ORDER BY
    date
LIMIT
    50
