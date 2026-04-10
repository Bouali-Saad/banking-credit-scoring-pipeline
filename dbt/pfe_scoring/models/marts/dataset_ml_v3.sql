{{ config(
    materialized='table',
    schema='marts'
) }}

WITH f AS (SELECT * FROM {{ ref('int_features_flag_transfo') }}),
     s AS (SELECT * FROM {{ ref('int_features_signaletique') }}),
     a AS (SELECT * FROM {{ ref('int_features_affaire') }}),
     c AS (SELECT * FROM {{ ref('int_features_ciblage') }}),
     sv AS (SELECT * FROM {{ ref('int_features_sav') }}),
     r AS (SELECT * FROM {{ ref('int_features_reclamation') }}),
     d AS (SELECT * FROM {{ ref('int_features_demande_info') }})

SELECT

    
    f.tiers_client,
    f.periode_trt,
    f.is_prediction_period,
    f.flag_transfo,
    f.date_trt_extr                             AS date_trt_extr_global,

    
    s.age,
    s.revenu,
    s.nbr_enfant,
    s.charges,
    s.mensualite_loyer,
    s.flag_eligible_md,
    s.csp_mkt,
    s.secteur_activite,
    s.civilite_client,
    s.prem_produit,
    s.canal_ent_relation,
    s.type_client,
    s.activite_profession,
    s.dernier_evt,
    s.anciennete_annees,
    s.anciennete_emploi,
    s.nb_jours_dernier_evt,

   
    a.nb_credits,
    a.moy_mt_init_brut,
    a.moy_mt_cap_rest,
    a.moy_montant_bien,
    a.moy_mt_apport,
    a.moy_mt_rachat_tot,
    a.moy_mt_vr,
    a.moy_mt_dg,
    a.total_nb_impaye,
    a.total_nb_impaye_regle,
    a.total_solde_impaye,
    a.max_nb_impaye,
    a.moy_taux_credit,
    a.moy_mensualite,
    a.moy_mensualite_av_der,
    a.moy_duree_initiale,
    a.moy_duree_actuelle,
    a.moy_nbr_ech_rest,
    a.moy_differe,
    a.flag_impaye,
    a.flag_contentieux,
    a.flag_credit_auto,
    a.flag_credit_equip,
    a.flag_credit_perso,
    a.flag_prel_prelevement,
    a.nb_credits_ctx,
    a.moy_retard_echeance,
    a.moy_delai_extraction                      AS aff_moy_delai_extraction,
    a.canal_prov_principal,
    a.code_reseau_principal,
    a.type_bien_principal,

    
    c.nb_campagnes,
    c.nb_contacts_total,
    c.nb_jours_cibles,
    c.duree_ciblage_jours,
    c.nb_sms_total,
    c.nb_sms_failed,
    c.nb_voice_total,
    c.nb_voice_failed,
    c.flag_canal_voice,

    
    sv.nb_sav,
    sv.nb_agences                               AS sav_nb_agences,
    sv.nb_affaires                              AS sav_nb_affaires,
    sv.flag_recouvrement,
    sv.flag_main_levee,
    sv.flag_modification,
    sv.flag_fidelisation,
    sv.flag_opposition,
    sv.flag_cloture                             AS sav_flag_cloture,
    sv.flag_changement_banque,
    sv.flag_situation_credit                    AS sav_flag_situation_credit,
    sv.flag_main_levee_auto,
    sv.flag_attestation_fin,
    sv.flag_report_echeance                     AS sav_flag_report_echeance,
    sv.flag_sav_actif,
    sv.flag_sav_cloture,
    sv.flag_sav_annule,
    sv.flag_canal_tel                           AS sav_flag_canal_tel,
    sv.nb_sav_non_clotures,
    sv.nb_sav_sans_delai,
    sv.moy_duree_traitement                     AS sav_moy_duree_traitement,
    sv.moy_delai_extraction                     AS sav_moy_delai_extraction,
    sv.date_premier_sav,
    sv.date_dernier_sav,

    
    r.nb_reclamations,
    r.flag_double_prelevement,
    r.nb_recla_non_clotures,
    r.date_premiere_recla,
    r.date_derniere_recla,

    
    d.nb_demandes,
    d.flag_demande_pret,
    d.flag_sav                                  AS dem_flag_sav,
    d.flag_simulation,
    d.flag_sort_dossier,
    d.flag_report_echeance                      AS dem_flag_report_echeance,
    d.flag_rachat_credit,
    d.moy_delai_extraction                      AS dem_moy_delai_extraction,
    d.date_premiere_demande,
    d.date_derniere_demande

FROM f


LEFT JOIN s  ON f.tiers_client    = s.tiers_client    AND f.periode_trt = s.periode_trt
LEFT JOIN a  ON f.tiers_client    = a.tiers_client    AND f.periode_trt = a.periode_trt


LEFT JOIN c  ON f.tiers_client    = c.tiers_client
LEFT JOIN sv ON s.id_tiers_siebel = sv.id_tiers_siebel
LEFT JOIN r  ON s.id_tiers_siebel = r.id_tiers_siebel
LEFT JOIN d  ON s.id_tiers_siebel = d.id_tiers_siebel