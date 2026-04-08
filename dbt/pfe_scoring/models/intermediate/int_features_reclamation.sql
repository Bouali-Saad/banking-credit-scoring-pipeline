WITH source AS (
    SELECT * FROM {{ ref('stg_reclamation') }}
)

SELECT
    id_tiers_siebel,
    periode_trt,

  
    COUNT(DISTINCT id_demande)              AS nb_reclamations,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%DOUBLE%PRELEV%'
             THEN 1 ELSE 0 END)             AS flag_double_prelevement,

    
    MIN(date_creation)                      AS date_premiere_recla,
    MAX(date_creation)                      AS date_derniere_recla,

   
    SUM(CASE WHEN date_fin IS NULL
             THEN 1 ELSE 0 END)             AS nb_recla_non_clotures,

    
    MIN(date_trt_extr)                      AS date_trt_extr

FROM source
WHERE id_tiers_siebel IS NOT NULL
GROUP BY id_tiers_siebel, periode_trt