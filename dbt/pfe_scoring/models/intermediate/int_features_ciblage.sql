

WITH source AS (
    SELECT * FROM {{ ref('stg_ciblage') }}
)

SELECT
    tiers_client,

    
    COUNT(DISTINCT rtc)                             AS nb_campagnes,
    COUNT(*)                                        AS nb_contacts_total,

    
    COUNT(DISTINCT periode_j)                       AS nb_jours_cibles,
    EXTRACT(DAY FROM
        MAX(TO_TIMESTAMP(
            NULLIF(TRIM(periode_j::text),''),
            'DDMONYYYY:HH24:MI:SS')) -
        MIN(TO_TIMESTAMP(
            NULLIF(TRIM(periode_j::text),''),
            'DDMONYYYY:HH24:MI:SS'))
    )                                               AS duree_ciblage_jours,

    
    SUM(nb_sms_recu)                                AS nb_sms_total,
    SUM(nb_sms_failed)                              AS nb_sms_failed,

    
    SUM(nb_voice_recu)                              AS nb_voice_total,
    SUM(nb_voice_failed)                            AS nb_voice_failed,

    
    MAX(CASE WHEN UPPER(flag_canal) = 'VOICE'
             THEN 1 ELSE 0 END)                     AS flag_canal_voice

FROM source
WHERE tiers_client IS NOT NULL
GROUP BY tiers_client