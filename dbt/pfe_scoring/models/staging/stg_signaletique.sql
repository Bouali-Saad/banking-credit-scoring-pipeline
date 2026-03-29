
WITH source AS (
    SELECT * FROM {{ source('raw', 'table_signaletique') }}
),

cleaned AS (
    SELECT
        TIERS_CLIENT                                AS tiers_client,
        ID_TIERS_SIEBEL                             AS id_tiers_siebel,
        NULLIF(TRIM(AGE_CLIENT), '')::numeric       AS age,
        NULLIF(TRIM(REVENU_MENSUEL), '')::numeric   AS revenu,
        NULLIF(TRIM(NBR_ENFANT), '')::numeric       AS nbr_enfant,
        NULLIF(TRIM(CHARGES_CLIE), '')::numeric     AS charges,
        CASE WHEN FLAG_ELIGIBLE_MD ~ '^[0-9]+$'
             THEN FLAG_ELIGIBLE_MD::numeric
             ELSE 0 END                             AS flag_eligible_md,
        TRIM(CSP_MKT)                               AS csp_mkt,
        TRIM(SECTEUR_ACTIVITE)                      AS secteur_activite,
        TRIM(CIVILITE_CLIENT)                       AS civilite_client,
        TRIM(PREM_PRODUIT)                          AS prem_produit,
        TRIM(CANAL_ENT_RELATION)                    AS canal_ent_relation,
        DATE_ENT_RELATION                           AS date_entree
    FROM source
    WHERE TIERS_CLIENT IS NOT NULL
)

SELECT * FROM cleaned