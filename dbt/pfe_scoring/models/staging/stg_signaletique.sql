WITH source AS (
    SELECT * FROM {{ source('raw', 'table_signaletique') }}
),

cleaned AS (
    SELECT
        TIERS_CLIENT                                AS tiers_client,
        ID_TIERS_SIEBEL                             AS id_tiers_siebel,
        TRIM(PERIODE_TRT)                           AS periode_trt,
       TO_TIMESTAMP(
          NULLIF(TRIM(DATE_TRT_EXTR::text),''),
               'DDMONYYYY:HH24:MI:SS'
                    )                                           AS date_trt_extr,

        NULLIF(TRIM(AGE_CLIENT), '')::numeric       AS age,
        NULLIF(TRIM(REVENU_MENSUEL), '')::numeric   AS revenu,
        NULLIF(TRIM(NBR_ENFANT), '')::numeric       AS nbr_enfant,
        NULLIF(TRIM(CHARGES_CLIE), '')::numeric     AS charges,
        NULLIF(TRIM(MENSUALITE_LOYER), '')::numeric AS mensualite_loyer,

        CASE WHEN FLAG_ELIGIBLE_MD ~ '^[0-9]+$'
             THEN FLAG_ELIGIBLE_MD::numeric
             ELSE 0 END                             AS flag_eligible_md,

        TRIM(CSP_MKT)                               AS csp_mkt,
        TRIM(SECTEUR_ACTIVITE)                      AS secteur_activite,
        TRIM(CIVILITE_CLIENT)                       AS civilite_client,
        TRIM(PREM_PRODUIT)                          AS prem_produit,
        TRIM(CANAL_ENT_RELATION)                    AS canal_ent_relation,
        TRIM(TYPE_CLIENT)                           AS type_client,
        TRIM(ACTIVITE_PROFESSION)                   AS activite_profession,
        TRIM(DERNIER_EVT)                           AS dernier_evt,
        TRIM(TYPE_PREL_PRIORITAIRE)                 AS type_prel_prioritaire,

        DATE_ENT_RELATION                           AS date_entree,
        DATE_DERNIER_EVT                            AS date_dernier_evt,
        DATE_EMBAUCHE                               AS date_embauche

    FROM source
    WHERE TIERS_CLIENT IS NOT NULL
    
    AND (
        DATE_TRT_EXTR IS NULL
        OR DATE_ENT_RELATION IS NULL
        OR DATE_TRT_EXTR::text >= DATE_ENT_RELATION::text
    )
)

SELECT * FROM cleaned