

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_sav') }}
),

cleaned AS (
    SELECT
        TRIM(ID_LANCEMENT)                      AS id_lancement,
        ID_TIERS_SIEBEL                         AS id_tiers_siebel,
        TRIM(PERIODE_TRT)                       AS periode_trt,
        DATE_TRT_EXTR                           AS date_trt_extr,
        TRIM(ID_DEMANDE)                        AS id_demande,
        TRIM(NUM_AFFAIRE)                       AS num_affaire,
        TRIM(TIERS_SOCIETE)                     AS tiers_societe,
        TRIM(AGENCE_CREATION)                   AS agence_creation,
        TRIM(SRC)                               AS src,

        
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
)

SELECT * FROM cleaned