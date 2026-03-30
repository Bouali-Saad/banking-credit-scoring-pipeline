-- ================================================================
-- STAGING : stg_affaire (Bronze)
-- Nettoyage table_affaire
-- ================================================================

WITH source AS (
    SELECT * FROM {{ source('raw', 'table_affaire') }}
),

cleaned AS (
    SELECT
        TIERS_CLIENT                                    AS tiers_client,
        NULLIF(TRIM(NB_IMPAYE), '')::numeric            AS nb_impaye,
        NULLIF(TRIM(SOLDE_IMPAYE), '')::numeric         AS solde_impaye,
        NULLIF(TRIM(TAUX_CREDIT), '')::numeric          AS taux_credit,
        NULLIF(TRIM(MENSUALITE), '')::numeric           AS mensualite,
        NULLIF(TRIM(MT_INIT_BRUT), '')::numeric        AS montant_init,
        NULLIF(TRIM(MT_CAP_REST), '')::numeric         AS capital_rest,
        NULLIF(TRIM(DUREE_INITIALE), '')::numeric      AS duree_initiale,
        NULLIF(TRIM(NBR_ECH_REST), '')::numeric        AS nbr_ech_rest,
        NULLIF(TRIM(NB_IMPAYE_REGLE), '')::numeric     AS nb_impaye_regle,
        TRIM(PRODUIT_WFS)                              AS produit_wfs,
        TRIM(CANAL_PROV)                               AS canal_prov
    FROM source
    WHERE TIERS_CLIENT IS NOT NULL
)

SELECT * FROM cleaned