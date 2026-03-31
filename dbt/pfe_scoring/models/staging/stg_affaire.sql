

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_affaire') }}
),

cleaned AS (
    SELECT
        TIERS_CLIENT                                        AS tiers_client,
        DATE_TRT_EXTR                                       AS date_trt_extr,
        TRIM(PERIODE_TRT)                                   AS periode_trt,
        TRIM(IE_AFFAIRE)                                    AS ie_affaire,

        
        NULLIF(TRIM(MT_INIT_BRUT), '')::numeric             AS montant_init_brut,
        NULLIF(TRIM(MT_INIT_NET), '')::numeric              AS montant_init_net,
        NULLIF(TRIM(MT_CAP_REST), '')::numeric              AS capital_rest,
        NULLIF(TRIM(MT_INTERETS), '')::numeric              AS mt_interets,
        NULLIF(TRIM(MT_ENCOURS_CTX), '')::numeric           AS mt_encours_ctx,
        NULLIF(TRIM(MT_APPORT_TTC), '')::numeric            AS mt_apport,
        NULLIF(TRIM(MT_VR), '')::numeric                    AS mt_vr,
        NULLIF(TRIM(MT_DG), '')::numeric                    AS mt_dg,
        NULLIF(TRIM(MONTANT_BIEN_TTC), '')::numeric         AS montant_bien,
        NULLIF(TRIM(MT_RACHAT_PART), '')::numeric           AS mt_rachat_part,
        NULLIF(TRIM(MT_RACHAT_TOT), '')::numeric            AS mt_rachat_tot,

        
        NULLIF(TRIM(NB_IMPAYE), '')::numeric                AS nb_impaye,
        NULLIF(TRIM(NB_IMPAYE_REGLE), '')::numeric          AS nb_impaye_regle,
        NULLIF(TRIM(SOLDE_IMPAYE), '')::numeric             AS solde_impaye,

        
        NULLIF(TRIM(TAUX_CREDIT), '')::numeric              AS taux_credit,
        NULLIF(TRIM(MENSUALITE), '')::numeric               AS mensualite,
        NULLIF(TRIM(MENSUALITE_AV_DER), '')::numeric        AS mensualite_av_der,
        NULLIF(TRIM(DUREE_INITIALE), '')::numeric           AS duree_initiale,
        NULLIF(TRIM(DUREE_ACTUELLE), '')::numeric           AS duree_actuelle,
        NULLIF(TRIM(NBR_ECH_REST), '')::numeric             AS nbr_ech_rest,
        NULLIF(TRIM(DIFFERE), '')::numeric                  AS differe,

        
        TRIM(PRODUIT_WFS)                                   AS produit_wfs,
        TRIM(CANAL_PROV)                                    AS canal_prov,
        TRIM(TYPE_PREL_ACTUEL)                              AS type_prel_actuel,
        TRIM(TYPE_PREL_INIT)                                AS type_prel_init,
        TRIM(TYPE_BIEN)                                     AS type_bien,
        TRIM(MARQUE_BIEN)                                   AS marque_bien,
        TRIM(MODELE_BIEN)                                   AS modele_bien,
        TRIM(CODE_RESEAU)                                   AS code_reseau,
        TRIM(MODIF_AFFAIRE)                                 AS modif_affaire,

        
        DATE_MEP                                            AS date_mep,
        DATE_ENTREE_CTX                                     AS date_entree_ctx,
        DATE_ECH_INIT                                       AS date_ech_init,
        DATE_ECH_REEL                                       AS date_ech_reel,
        DATE_MODIF_AFFAIRE                                  AS date_modif_affaire

    FROM source
    WHERE TIERS_CLIENT IS NOT NULL
)

SELECT * FROM cleaned
