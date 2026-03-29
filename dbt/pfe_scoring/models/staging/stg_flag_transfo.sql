WITH source AS (
    SELECT * FROM {{ source('raw', 'flag_transfo') }}
),

cleaned AS (
    SELECT
        TIERS_CLIENT                    AS tiers_client,
        PERIODE_TRT                     AS periode_trt,
        flag_transfo::int               AS flag_transfo,
        CASE WHEN PERIODE_TRT = '012026'
             THEN 1 ELSE 0 END          AS is_prediction_period
    FROM source
    WHERE TIERS_CLIENT IS NOT NULL
    AND PERIODE_TRT IS NOT NULL
)

SELECT * FROM cleaned