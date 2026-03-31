

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_ciblage') }}
),

cleaned AS (
    SELECT
        ID_TIER                                         AS tiers_client,
        TRIM(PERIODE_J)                                 AS periode_j,
        NULLIF(TRIM(NB_SMS_RECU_J), '')::numeric       AS nb_sms_recu,
        NULLIF(TRIM(NB_SMS_FAILED_J), '')::numeric     AS nb_sms_failed,
        NULLIF(TRIM(NB_SMS_RETRY_J), '')::numeric      AS nb_sms_retry,
        NULLIF(TRIM(NB_APPELS_RECU_MSGVOCALE_J), '')::numeric
                                                        AS nb_voice_recu,
        TRIM(FLAG)                                      AS flag_canal,
        TRIM(RTC)                                       AS rtc
    FROM source
    WHERE ID_TIER IS NOT NULL
)

SELECT * FROM cleaned