WITH source AS (
    SELECT * FROM {{ ref('stg_demande_info')}}

)


SELECT
    id_tiers_siebel,
    periode_trt,
    date_trt_extr,
    id_lancement,
    id_demande,
    agence_creation,
    categorie,
    sous_categorie,
    canal,
    flag_copie,
    date_creation
FROM source
WHERE id_tiers_siebel IS NOT NULL