WITH source AS (
    SELECT * FROM {{ ref('stg_ciblage')}}
)

SELECT
    tiers_client,
    periode_j,
    nb_sms_recu,
    nb_sms_failed,
    nb_sms_retry,
    nb_appels_tmk_recu,
    nb_appels_tmk_failed,
    nb_voice_recu,
    nb_voice_failed,
    flag_canal,
    rtc,
    insert_date
FROM source
WHERE tiers_client IS NOT NULL