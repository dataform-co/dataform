SELECT
    *
FROM
    `dataform-open-source.was_there_extreme_weather`
    LEFT OUTER JOIN `dataform-open-source.repositories_that_mention_extreme_weather` USING (date)
ORDER BY
    date
