
WITH source AS (
    SELECT * FROM {{ ref('stg_affaire') }}
)

SELECT
    tiers_client,
    periode_trt,

    -- ═══ COMPTAGES ═══
    COUNT(DISTINCT ie_affaire)                              AS nb_credits,

    -- ═══ MONTANTS (NULL = erreur → garder NULL) ═══
    AVG(montant_init_brut)                                  AS moy_mt_init_brut,
    AVG(capital_rest)                                       AS moy_mt_cap_rest,
    AVG(montant_bien)                                       AS moy_montant_bien,

    -- ═══ MONTANTS (NULL = logique → COALESCE 0) ═══
    AVG(COALESCE(mt_apport, 0))                             AS moy_mt_apport,
    AVG(COALESCE(mt_rachat_tot, 0))                         AS moy_mt_rachat_tot,
    AVG(COALESCE(mt_vr, 0))                                 AS moy_mt_vr,
    AVG(COALESCE(mt_dg, 0))                                 AS moy_mt_dg,

    -- ═══ IMPAYÉS (NULL = 0 logique) ═══
    SUM(COALESCE(nb_impaye, 0))                             AS total_nb_impaye,
    SUM(COALESCE(nb_impaye_regle, 0))                       AS total_nb_impaye_regle,
    SUM(COALESCE(solde_impaye, 0))                          AS total_solde_impaye,
    MAX(COALESCE(nb_impaye, 0))                             AS max_nb_impaye,

    -- ═══ TAUX (NULL = erreur → garder NULL) ═══
    AVG(taux_credit)                                        AS moy_taux_credit,

    -- ═══ MENSUALITÉS (NULL = LOA → COALESCE 0) ═══
    AVG(COALESCE(mensualite, 0))                            AS moy_mensualite,
    AVG(COALESCE(mensualite_av_der, 0))                     AS moy_mensualite_av_der,

    -- ═══ DURÉES (NULL = erreur → garder NULL) ═══
    AVG(duree_initiale)                                     AS moy_duree_initiale,
    AVG(duree_actuelle)                                     AS moy_duree_actuelle,
    AVG(nbr_ech_rest)                                       AS moy_nbr_ech_rest,
    AVG(COALESCE(differe, 0))                               AS moy_differe,

    -- ═══ FLAGS IMPAYÉS ═══
    MAX(CASE WHEN COALESCE(nb_impaye, 0) > 0
             THEN 1 ELSE 0 END)                             AS flag_impaye,
    MAX(CASE WHEN COALESCE(mt_encours_ctx, 0) > 0
             THEN 1 ELSE 0 END)                             AS flag_contentieux,

    -- ═══ FLAGS PRODUIT ═══
    MAX(CASE WHEN UPPER(produit_wfs) LIKE '%AUTO%'
             THEN 1 ELSE 0 END)                             AS flag_credit_auto,
    MAX(CASE WHEN UPPER(produit_wfs) LIKE '%EQUIPEMENT%'
             THEN 1 ELSE 0 END)                             AS flag_credit_equip,
    MAX(CASE WHEN UPPER(produit_wfs) LIKE '%PRET%'
             OR UPPER(produit_wfs) LIKE '%PERSONNEL%'
             THEN 1 ELSE 0 END)                             AS flag_credit_perso,

    -- ═══ FLAG PRÉLÈVEMENT ═══
    MAX(CASE WHEN UPPER(type_prel_actuel)
             LIKE '%PRELEVEMENT%'
             THEN 1 ELSE 0 END)                             AS flag_prel_prelevement,

    -- ═══ CONTENTIEUX ═══
    SUM(CASE WHEN date_entree_ctx IS NOT NULL
             THEN 1 ELSE 0 END)                             AS nb_credits_ctx,

    -- ═══ RETARD ÉCHÉANCE ═══
    AVG(CASE WHEN date_ech_reel IS NOT NULL
             AND date_ech_init IS NOT NULL
             THEN EXTRACT(DAY FROM
                  TO_TIMESTAMP(NULLIF(TRIM(date_ech_reel::text),''),
                               'DDMONYYYY:HH24:MI:SS') -
                  TO_TIMESTAMP(NULLIF(TRIM(date_ech_init::text),''),
                               'DDMONYYYY:HH24:MI:SS'))
             ELSE NULL END)                                 AS moy_retard_echeance,

    -- ═══ DÉLAI EXTRACTION ═══
    AVG(CASE WHEN date_trt_extr IS NOT NULL
             AND date_mep IS NOT NULL
             THEN EXTRACT(DAY FROM
                  TO_TIMESTAMP(NULLIF(TRIM(date_trt_extr::text),''),
                               'DDMONYYYY:HH24:MI:SS') -
                  TO_TIMESTAMP(NULLIF(TRIM(date_mep::text),''),
                               'DDMONYYYY:HH24:MI:SS'))
             ELSE NULL END)                                 AS moy_delai_extraction,

    -- ═══ CATÉGORIELLES ═══
    MODE() WITHIN GROUP (ORDER BY canal_prov)               AS canal_prov_principal,
    MODE() WITHIN GROUP (ORDER BY code_reseau)              AS code_reseau_principal,
    MODE() WITHIN GROUP (ORDER BY type_bien)                AS type_bien_principal

FROM source
WHERE tiers_client IS NOT NULL
GROUP BY tiers_client, periode_trt