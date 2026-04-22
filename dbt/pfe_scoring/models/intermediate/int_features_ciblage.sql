WITH source AS (
    SELECT * FROM {{ ref('stg_ciblage') }}
),
flag AS (
    SELECT tiers_client, periode_trt, date_trt_extr
    FROM {{ ref('int_features_flag_transfo') }}
)

SELECT
    f.tiers_client,
    f.periode_trt,
    COUNT(DISTINCT s.rtc)         AS nb_campagnes,
    COUNT(s.rtc)                  AS nb_contacts_total,
    COUNT(DISTINCT s.periode_j)   AS nb_jours_cibles,
    EXTRACT(DAY FROM
        MAX(TO_TIMESTAMP(NULLIF(TRIM(s.periode_j::text),''),
                         'DDMONYYYY:HH24:MI:SS')) -
        MIN(TO_TIMESTAMP(NULLIF(TRIM(s.periode_j::text),''),
                         'DDMONYYYY:HH24:MI:SS'))
    )                             AS duree_ciblage_jours,
    SUM(s.nb_sms_recu)            AS nb_sms_total,
    SUM(s.nb_sms_failed)          AS nb_sms_failed,
    SUM(s.nb_voice_recu)          AS nb_voice_total,
    SUM(s.nb_voice_failed)        AS nb_voice_failed,
    MAX(CASE WHEN UPPER(s.flag_canal) = 'VOICE'
             THEN 1 ELSE 0 END)   AS flag_canal_voice

FROM flag f
LEFT JOIN source s 
    ON f.tiers_client = s.tiers_client
    AND TO_TIMESTAMP(NULLIF(TRIM(s.periode_j::text),''),
                     'DDMONYYYY:HH24:MI:SS') < f.date_trt_extr

WHERE f.tiers_client IS NOT NULL
GROUP BY f.tiers_client, f.periode_trt