SELECT
    *
FROM
    ${ ref("was_there_extreme_weather") }
    LEFT OUTER JOIN ${ ref("repositories_that_mention_extreme_weather") } USING (date)
ORDER BY
    date
