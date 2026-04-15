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
    f.date_trt_extr                                     AS date_trt_extr_global,

    
    s.age,
    s.revenu,
    s.nbr_enfant,
    s.charges,
    s.mensualite_loyer,
    s.flag_eligible_md,
    s.anciennete_annees,
    s.anciennete_emploi,
    s.nb_jours_dernier_evt,

   
    s.csp_mkt,           
    s.type_client,       
   
    CASE
        WHEN s.civilite_client = 'Monsieur'                     THEN 'HOMME'
        WHEN s.civilite_client IN ('Madame', 'Mademoiselle')    THEN 'FEMME'
        WHEN s.civilite_client IS NULL                          THEN 'INCONNU'
        ELSE                                                         'PERSONNE_MORALE'
    END                                                         AS groupe_civilite,

    
    CASE
        WHEN s.prem_produit = 'Credit des menages'              THEN 'CREDIT_MENAGES'
        WHEN s.prem_produit = 'Credit Auto'                     THEN 'CREDIT_AUTO'
        WHEN s.prem_produit IN ('Toutes affaires LOA',
                                'Affaire LOA')                  THEN 'LOA'
        WHEN s.prem_produit = 'Credit Revolving'                THEN 'REVOLVING'
        WHEN s.prem_produit IS NULL                             THEN 'INCONNU'
        ELSE                                                         'AUTRES_PRODUITS'
    END                                                         AS groupe_produit,

   
    CASE
        WHEN s.dernier_evt = 'Demande de credit'                THEN 'DEMANDE_CREDIT'
        WHEN s.dernier_evt = 'Demande de reclamation'           THEN 'RECLAMATION'
        WHEN s.dernier_evt IS NULL                              THEN 'INCONNU'
        ELSE                                                         'AUTRE_EVT'
    END                                                         AS groupe_dernier_evt,

    
    CASE
        WHEN a.canal_prov_principal IN ('MAILING','TELEMARKET','AFFICHAGE',
                                        'DEPLIANT','BOUCHE A O','ENTREPRISE',
                                        'SALON')                THEN 'CANAL_ACTIF'
        WHEN a.canal_prov_principal IN ('ARCW','ARCD')          THEN 'CANAL_ARC'
        WHEN a.canal_prov_principal IN ('PARRAINNAE','PRON_PAS','TV','NO VERT',
                                        'INTERNET','PRESSE','1118','RADIO',
                                        '1103','CRC','SMS')     THEN 'CANAL_INACTIF'
        WHEN a.canal_prov_principal IS NULL                     THEN 'INCONNU'
        ELSE                                                         'AUTRES'
    END                                                         AS groupe_canal_prov,

    
    CASE
        WHEN a.code_reseau_principal IN ('BMW','AUDI','MERC','ALFA','JAGU',
                                         'LAND','VOLV','ROVE','CADI')
                                                                THEN 'PREMIUM'
        WHEN a.code_reseau_principal IN ('RENA','PEUG','CITR','FIAT','HYUN',
                                         'TOYO','DACI','OPEL','SEAT','SKOD',
                                         'FORD','VOLK')         THEN 'POPULAIRE'
        WHEN a.code_reseau_principal IN ('KIA','MITS','NISS','HOND','SUZU',
                                         'MAZD','DAEW','DAIH','SSAN')
                                                                THEN 'ASIATIQUE'
        WHEN a.code_reseau_principal IN ('MARJ','AUT2','DIGI','AEDM','E130',
                                         'E250','E300','E400','E450','E500',
                                         'E600','E700','E730','E800','E820',
                                         'E960')                THEN 'RESEAU_BANQUE'
        WHEN a.code_reseau_principal IS NULL                    THEN 'INCONNU'
        ELSE                                                         'AUTRES'
    END                                                         AS groupe_reseau,

   
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
    a.moy_delai_extraction                                      AS aff_moy_delai_extraction,

    
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
    sv.nb_agences                                               AS sav_nb_agences,
    sv.nb_affaires                                              AS sav_nb_affaires,
    sv.flag_recouvrement,
    sv.flag_main_levee,
    sv.flag_modification,
    sv.flag_fidelisation,
    sv.flag_opposition,
    sv.flag_cloture                                             AS sav_flag_cloture,
    sv.flag_changement_banque,
    sv.flag_situation_credit                                    AS sav_flag_situation_credit,
    sv.flag_main_levee_auto,
    sv.flag_attestation_fin,
    sv.flag_report_echeance                                     AS sav_flag_report_echeance,
    sv.flag_sav_actif,
    sv.flag_sav_cloture,
    sv.flag_sav_annule,
    sv.flag_canal_tel                                           AS sav_flag_canal_tel,
    sv.nb_sav_non_clotures,
    sv.nb_sav_sans_delai,
    sv.moy_duree_traitement                                     AS sav_moy_duree_traitement,
    sv.moy_delai_extraction                                     AS sav_moy_delai_extraction,

    
    r.nb_reclamations,
    r.flag_double_prelevement,
    r.nb_recla_non_clotures,

    
    d.nb_demandes,
    d.flag_demande_pret,
    d.flag_sav                                                  AS dem_flag_sav,
    d.flag_simulation,
    d.flag_sort_dossier,
    d.flag_report_echeance                                      AS dem_flag_report_echeance,
    d.flag_rachat_credit,
    d.moy_delai_extraction                                      AS dem_moy_delai_extraction,

   
    CASE
        WHEN s.revenu IS NULL OR s.revenu = 0 THEN NULL
        ELSE ROUND((a.moy_mensualite / s.revenu)::numeric, 4)
    END                                                         AS taux_endettement,

    
    COALESCE(a.flag_impaye, 0)
    + COALESCE(sv.flag_recouvrement, 0)
    + COALESCE(a.flag_contentieux, 0)                          AS score_risque,

    
    CASE
        WHEN a.moy_mt_init_brut IS NULL OR a.moy_mt_init_brut = 0 THEN NULL
        ELSE ROUND((a.moy_mt_cap_rest / a.moy_mt_init_brut)::numeric, 4)
    END                                                         AS ratio_remboursement,

   
    CASE
        WHEN s.anciennete_annees IS NULL THEN NULL
        WHEN s.anciennete_annees < 2     THEN 1
        ELSE 0
    END                                                         AS flag_nouveau_client,

    
    CASE
        WHEN sv.date_dernier_sav IS NULL THEN NULL
        WHEN TO_TIMESTAMP(NULLIF(TRIM(sv.date_dernier_sav::text),''),
                          'DDMONYYYY:HH24:MI:SS') > f.date_trt_extr THEN NULL
        ELSE EXTRACT(DAY FROM f.date_trt_extr -
                     TO_TIMESTAMP(NULLIF(TRIM(sv.date_dernier_sav::text),''),
                                  'DDMONYYYY:HH24:MI:SS'))::numeric
    END                                                         AS recence_sav,

    
    CASE
        WHEN r.date_derniere_recla IS NULL THEN NULL
        WHEN TO_TIMESTAMP(NULLIF(TRIM(r.date_derniere_recla::text),''),
                          'DDMONYYYY:HH24:MI:SS') > f.date_trt_extr THEN NULL
        ELSE EXTRACT(DAY FROM f.date_trt_extr -
                     TO_TIMESTAMP(NULLIF(TRIM(r.date_derniere_recla::text),''),
                                  'DDMONYYYY:HH24:MI:SS'))::numeric
    END                                                         AS recence_recla,

   
    CASE
        WHEN d.date_derniere_demande IS NULL THEN NULL
        WHEN TO_TIMESTAMP(NULLIF(TRIM(d.date_derniere_demande::text),''),
                          'DDMONYYYY:HH24:MI:SS') > f.date_trt_extr THEN NULL
        ELSE EXTRACT(DAY FROM f.date_trt_extr -
                     TO_TIMESTAMP(NULLIF(TRIM(d.date_derniere_demande::text),''),
                                  'DDMONYYYY:HH24:MI:SS'))::numeric
    END                                                         AS recence_demande

FROM f


LEFT JOIN s  ON f.tiers_client    = s.tiers_client    AND f.periode_trt = s.periode_trt
LEFT JOIN a  ON f.tiers_client    = a.tiers_client    AND f.periode_trt = a.periode_trt


LEFT JOIN c  ON f.tiers_client    = c.tiers_client
LEFT JOIN sv ON s.id_tiers_siebel = sv.id_tiers_siebel
LEFT JOIN r  ON s.id_tiers_siebel = r.id_tiers_siebel
LEFT JOIN d  ON s.id_tiers_siebel = d.id_tiers_siebel