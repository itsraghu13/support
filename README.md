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
