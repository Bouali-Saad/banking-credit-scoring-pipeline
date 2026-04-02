

WITH source AS (
    SELECT * FROM {{ ref('stg_affaire') }}
)

SELECT
    tiers_client,
    periode_trt,
    date_trt_extr,
    ie_affaire,
    montant_init_brut,
    montant_init_net,
    capital_rest,
    mt_interets,
    mt_encours_ctx,
    mt_apport,
    mt_vr,
    mt_dg,
    montant_bien,
    mt_rachat_part,
    mt_rachat_tot,
    nb_impaye,
    nb_impaye_regle,
    solde_impaye,
    taux_credit,
    mensualite,
    mensualite_av_der,
    duree_initiale,
    duree_actuelle,
    nbr_ech_rest,
    differe,
    produit_wfs,
    canal_prov,
    type_prel_actuel,
    type_prel_init,
    type_bien,
    marque_bien,
    modele_bien,
    code_reseau,
    modif_affaire,
    date_mep,
    date_entree_ctx,
    date_ech_init,
    date_ech_reel,
    date_modif_affaire
FROM source
WHERE tiers_client IS NOT NULL