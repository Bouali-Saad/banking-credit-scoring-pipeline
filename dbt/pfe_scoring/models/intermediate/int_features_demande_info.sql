

WITH source AS (
    SELECT * FROM {{ ref('stg_demande_info') }}
)

SELECT
    id_tiers_siebel,
    
    COUNT(DISTINCT id_demande)                      AS nb_demandes,

    
    MAX(CASE WHEN UPPER(categorie)
             LIKE '%PRET%'
             THEN 1 ELSE 0 END)                     AS flag_demande_pret,

    MAX(CASE WHEN UPPER(categorie)
             LIKE '%SAV%'
             THEN 1 ELSE 0 END)                     AS flag_sav,

    -- ═══ FLAGS SOUS_CATEGORIE ═══
    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%SIMULATION%'
             THEN 1 ELSE 0 END)                     AS flag_simulation,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%SORT%DOSSIER%'
             THEN 1 ELSE 0 END)                     AS flag_sort_dossier,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%REPORT%ECHEANCE%'
             THEN 1 ELSE 0 END)                     AS flag_report_echeance,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%RACHAT%'
             THEN 1 ELSE 0 END)                     AS flag_rachat_credit,

    
    AVG(EXTRACT(DAY FROM
    date_trt_extr -
    TO_TIMESTAMP(
        NULLIF(TRIM(date_creation::text), ''),
        'DDMONYYYY:HH24:MI:SS')
    ))                                              AS moy_delai_extraction,

     

    
    MIN(date_creation)                              AS date_premiere_demande,
    MAX(date_creation)                              AS date_derniere_demande

    
    

FROM source
WHERE id_tiers_siebel IS NOT NULL
GROUP BY id_tiers_siebel