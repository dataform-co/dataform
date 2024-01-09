SELECT
    DATE_TRUNC(DATE(repository_created_at), MONTH) AS date,
    COUNT(*) AS repository_count
FROM
    `bigquery-public-data.samples.github_timeline`
WHERE
    REGEXP_CONTAINS(
        repository_description,
        "${constants.WEATHER.join(" | ")}"
    )
GROUP BY
    date
ORDER BY
    date
