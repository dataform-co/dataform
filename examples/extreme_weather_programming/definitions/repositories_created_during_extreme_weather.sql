SELECT
    *
FROM
    `dataform-demos.was_there_extreme_weather`
    LEFT OUTER JOIN `dataform-demos.repositories_that_mention_extreme_weather` USING (date)
ORDER BY
    date
