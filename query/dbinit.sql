CREATE OR REPLACE TABLE frc2025 AS SELECT * FROM read_parquet('s3://robologs/year=2025/**/**/*.parquet', union_by_name = true, filename = true, hive_partitioning = true);

CREATE OR REPLACE MACRO MATCH_TYPE_STRING(X) AS CASE
    WHEN X = 2 THEN 'q'
    WHEN X = 0 THEN 'p'
    WHEN X = 3 THEN 'e'
    ELSE '<?>'
END;

CREATE MACRO IF NOT EXISTS LINE_TO_POINT(A, B, C, x, y) AS ABS(A*x + B*y + C) / SQRT(POWER(A, 2) + POWER(B,2));
