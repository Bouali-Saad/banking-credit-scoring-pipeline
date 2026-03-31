

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_reclamation') }}
),

cleaned AS (
    SELECT
        ID_TIERS_SIEBEL                         AS id_tiers_siebel,
        TRIM(CATEGORIE)                         AS categorie,
        TRIM(SOUS_CATEGORIE)                    AS sous_categorie,
        TRIM(STATUT_DEMANDE)                    AS statut_demande,
        TRIM(CANAL)                             AS canal,
        FLAG_COPIE                              AS flag_copie,
        DATE_CREATION                           AS date_creation
    FROM source
    WHERE ID_TIERS_SIEBEL IS NOT NULL
    AND FLAG_COPIE = 'O'
)

SELECT * FROM cleaned