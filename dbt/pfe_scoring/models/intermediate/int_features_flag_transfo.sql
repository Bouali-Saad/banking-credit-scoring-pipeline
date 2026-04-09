

WITH source AS (
    SELECT * FROM {{ ref('stg_flag_transfo') }}
)

SELECT
    tiers_client,
    periode_trt,
    date_trt_extr,
    flag_transfo,
    is_prediction_period
FROM source
WHERE tiers_client IS NOT NULL
AND periode_trt IS NOT NULL