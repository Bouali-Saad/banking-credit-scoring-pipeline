WITH source AS (
    SELECT * FROM {{ source('raw', 'table_reclamation') }}
),

cleaned AS (
    SELECT
        TRIM(ID_LANCEMENT)                      AS id_lancement,
        ID_TIERS_SIEBEL                         AS id_tiers_siebel,
        TRIM(PERIODE_TRT)                       AS periode_trt,
        TO_TIMESTAMP(
                         NULLIF(TRIM(DATE_TRT_EXTR::text),''),
                        'DDMONYYYY:HH24:MI:SS'
                    )                                           AS date_trt_extr,
        TRIM(ID_DEMANDE)                        AS id_demande,
        TRIM(NUM_AFFAIRE)                       AS num_affaire,
        TRIM(TIERS_SOCIETE)                     AS tiers_societe,
        TRIM(AGENCE_CREATION)                   AS agence_creation,

        TRIM(CATEGORIE)                         AS categorie,
        TRIM(SOUS_CATEGORIE)                    AS sous_categorie,
        TRIM(STATUT_DEMANDE)                    AS statut_demande,
        TRIM(CANAL)                             AS canal,
        FLAG_COPIE                              AS flag_copie,

        DATE_CREATION                           AS date_creation,
        DATE_FIN                                AS date_fin,
        DATE_FIN_THEORIQUE                      AS date_fin_theorique

    FROM source
    WHERE ID_TIERS_SIEBEL IS NOT NULL
    AND FLAG_COPIE = 'O'
    
    AND (
        DATE_TRT_EXTR IS NULL
        OR DATE_CREATION IS NULL
        OR DATE_TRT_EXTR::text >= DATE_CREATION::text
    )
)

SELECT * FROM cleaned