SELECT
    byte_value AS bytes,
    CASE
        WHEN byte_value >= 1024 THEN CAST(byte_value / 1024.0 AS DECIMAL(18, 2))
        ELSE 0.0
    END AS kb,
    CASE
        WHEN byte_value >= 1024 * 1024 THEN CAST(byte_value / (1024.0 * 1024.0) AS DECIMAL(18, 2))
        ELSE 0.0
    END AS mb,
    CASE
        WHEN byte_value >= 1024 * 1024 * 1024 THEN CAST(byte_value / (1024.0 * 1024.0 * 1024.0) AS DECIMAL(18, 2))
        ELSE 0.0
    END AS gb
FROM
    (SELECT 1500000 AS byte_value) -- Replace this with your byte value





-- Register the DataFrame as a temporary table
CREATE OR REPLACE TEMPORARY VIEW your_temp_table AS
SELECT *
FROM yourDataFrame;

-- Use Spark SQL to convert the column to JSON
SELECT
  map_from_entries(
    transform(
      filter(
        transform(
          split(yourColumn, '\\|'),
          x -> split(x, ':')
        ),
        x -> size(x) = 2
      ),
      x -> (x[0], x[1])
    )
  ) AS json_result
FROM your_temp_table;

