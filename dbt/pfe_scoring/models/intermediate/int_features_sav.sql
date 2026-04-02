WITH source AS (
    SELECT * FROM {{ ref('stg_sav')}}
)


SELECT
    id_lancement,
    id_tiers_siebel,
    periode_trt,
    date_trt_extr,
    id_demande,
    num_affaire,
    tiers_societe,
    agence_creation,
    src,
    categorie,
    sous_categorie,
    statut_demande,
    canal,
    flag_copie,
    date_creation,
    date_fin,
    date_fin_theorique
FROM source
WHERE id_tiers_siebel IS NOT NULL