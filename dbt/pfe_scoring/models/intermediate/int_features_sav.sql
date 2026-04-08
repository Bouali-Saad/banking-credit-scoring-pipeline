
WITH source AS (
    SELECT * FROM {{ ref('stg_sav') }}
)

SELECT
    id_tiers_siebel,
    periode_trt,

    -- ═══ COMPTAGES ═══
    COUNT(*)                                        AS nb_sav,
    COUNT(DISTINCT agence_creation)                 AS nb_agences,
    COUNT(DISTINCT num_affaire)                     AS nb_affaires,

    -- ═══ FLAGS CATEGORIE ═══
    MAX(CASE WHEN UPPER(categorie)
             LIKE '%RECOUVREMENT%'
             THEN 1 ELSE 0 END)                     AS flag_recouvrement,

    MAX(CASE WHEN UPPER(categorie)
             LIKE '%MAIN%LEVEE%'
             THEN 1 ELSE 0 END)                     AS flag_main_levee,

    MAX(CASE WHEN UPPER(categorie)
             LIKE '%MODIFICATION%'
             THEN 1 ELSE 0 END)                     AS flag_modification,

    MAX(CASE WHEN UPPER(categorie)
             LIKE '%FIDELISATION%'
             THEN 1 ELSE 0 END)                     AS flag_fidelisation,

    -- ═══ FLAGS SOUS_CATEGORIE ═══
    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%OPPOSITION%'
             THEN 1 ELSE 0 END)                     AS flag_opposition,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%CLOTURE%'
             THEN 1 ELSE 0 END)                     AS flag_cloture,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%CHANGEMENT%BANQUE%'
             THEN 1 ELSE 0 END)                     AS flag_changement_banque,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%SITUATION%CREDIT%'
             THEN 1 ELSE 0 END)                     AS flag_situation_credit,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%MAIN%LEVEE%'
             THEN 1 ELSE 0 END)                     AS flag_main_levee_auto,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%ATTESTATION%'
             THEN 1 ELSE 0 END)                     AS flag_attestation_fin,

    MAX(CASE WHEN UPPER(sous_categorie)
             LIKE '%REPORT%ECHEANCE%'
             THEN 1 ELSE 0 END)                     AS flag_report_echeance,

    -- ═══ FLAGS STATUT ═══
    MAX(CASE WHEN UPPER(statut_demande)
             IN ('OUVERTE', 'EN COURS',
                 'EN ATTENTE', 'INITIEE',
                 'EN_COURS_TRAITEMENT',
                 'EN_ATTENTE_COMPLEMENT')
             THEN 1 ELSE 0 END)                     AS flag_sav_actif,

    MAX(CASE WHEN UPPER(statut_demande)
             IN ('CLOTUREE', 'TRAITEE',
                 'TRAITE_AVEC_FORCAGE')
             THEN 1 ELSE 0 END)                     AS flag_sav_cloture,

    MAX(CASE WHEN UPPER(statut_demande)
             IN ('ANNULEE', 'REJETEE',
                 'EN_ECHEC', 'DOUBLON')
             THEN 1 ELSE 0 END)                     AS flag_sav_annule,

    -- ═══ FLAGS CANAL ═══
    MAX(CASE WHEN UPPER(canal) LIKE '%TEL%'
             THEN 1 ELSE 0 END)                     AS flag_canal_tel,

    
    MIN(date_creation)                              AS date_premier_sav,
    MAX(date_creation)                              AS date_dernier_sav,

    -- ═══ DURÉE TRAITEMENT ═══
    AVG(CASE WHEN date_fin IS NOT NULL
             THEN EXTRACT(DAY FROM
                  TO_TIMESTAMP(NULLIF(TRIM(date_fin::text), ''),
                               'DDMONYYYY:HH24:MI:SS') -
                  TO_TIMESTAMP(NULLIF(TRIM(date_creation::text), ''),
                               'DDMONYYYY:HH24:MI:SS'))
             ELSE NULL END)                         AS moy_duree_traitement,

    
    SUM(CASE WHEN date_fin IS NULL
             THEN 1 ELSE 0 END)                     AS nb_sav_non_clotures,

    
    SUM(CASE WHEN date_fin_theorique IS NULL
             THEN 1 ELSE 0 END)                     AS nb_sav_sans_delai,

    
    AVG(EXTRACT(DAY FROM
    date_trt_extr -
    TO_TIMESTAMP(NULLIF(TRIM(date_creation::text), ''),
                 'DDMONYYYY:HH24:MI:SS')
))                                              AS moy_delai_extraction,

MIN(date_trt_extr)                              AS date_trt_extr


FROM source
WHERE id_tiers_siebel IS NOT NULL
GROUP BY id_tiers_siebel, periode_trt