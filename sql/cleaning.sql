CREATE OR REPLACE TABLE 
  `my-healthcare-project-487800.Healthplan_analysis.enrollments_cleaned_p5` AS

WITH base AS (
    SELECT
        -- Standardize text fields
        TRIM(UPPER(age_group)) AS age_group,
        TRIM(UPPER(gender)) AS gender,
        TRIM(quarter) AS quarter,
        SAFE_CAST(quarter_num AS INT64) AS quarter_num,
        TRIM(reporting_period) AS reporting_period,

        -- Numeric fields
        SAFE_CAST(year AS INT64) AS year,
        TRIM(year_quarter) AS year_quarter,
        SAFE_CAST(enrollment AS INT64) AS enrollment,
        SAFE_CAST(yoy_growth AS FLOAT64) AS yoy_growth,
        SAFE_CAST(rolling_4q_avg AS FLOAT64) AS rolling_4q_avg,
        SAFE_CAST(share_of_quarter AS FLOAT64) AS share_of_quarter,
        SAFE_CAST(gender_ratio AS FLOAT64) AS gender_ratio,
        SAFE_CAST(seasonality_index AS FLOAT64) AS seasonality_index,
        SAFE_CAST(hhi_concentration AS FLOAT64) AS hhi_concentration
    FROM 
        `my-healthcare-project-487800.Healthplan_analysis.enrollments_engineered`
),

-- Remove duplicate rows (your dataset has many)
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY age_group, gender, year, quarter_num
            ORDER BY enrollment DESC
        ) AS rn
    FROM base
)

SELECT
    year,
    reporting_period,
    quarter,
    age_group,
    gender,
    enrollment,
    year_quarter,
    quarter_num,
    yoy_growth,
    rolling_4q_avg,
    share_of_quarter,
    gender_ratio,
    seasonality_index,
    hhi_concentration
FROM dedup
WHERE rn = 1
ORDER BY year, quarter_num, age_group, gender;
