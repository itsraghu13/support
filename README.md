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





SELECT
  json_object_agg(split_parts[1], split_parts[2]) AS json_result
FROM (
  SELECT
    regexp_split_to_array(key_value_pairs, ':') AS split_parts
  FROM (
    SELECT
      regexp_split_to_array(your_column, '\|') AS key_value_pairs
    FROM your_table
  ) subquery
) subquery2;

