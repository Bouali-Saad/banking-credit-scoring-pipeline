SET search_path TO raw, public;


SELECT 'table_signaletique' AS table_name, COUNT(*) AS nb_lignes FROM raw.table_signaletique
UNION ALL
SELECT 'table_reclamation',  COUNT(*) FROM raw.table_reclamation
UNION ALL
SELECT 'table_sav',          COUNT(*) FROM raw.table_sav
UNION ALL
SELECT 'table_ciblage',      COUNT(*) FROM raw.table_ciblage
UNION ALL
SELECT 'table_demande_info', COUNT(*) FROM raw.table_demande_info
UNION ALL
SELECT 'table_affaire',      COUNT(*) FROM raw.table_affaire
UNION ALL
SELECT 'flag_transfo',       COUNT(*) FROM raw.flag_transfo
ORDER BY table_name;


------------------------------------------------------------------------------------------------
SELECT
    flag_transfo,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.flag_transfo
GROUP BY flag_transfo
ORDER BY flag_transfo;

------------------------------------------------------------------------------------------------
SELECT COUNT(DISTINCT TIERS_CLIENT) AS nb_clients_uniques
FROM raw.flag_transfo;

------------------------------------------------------------------------------------------------
SELECT
    PERIODE_TRT,
    flag_transfo,
    COUNT(*) AS nb_clients
FROM raw.flag_transfo
GROUP BY PERIODE_TRT, flag_transfo
ORDER BY PERIODE_TRT, flag_transfo;


-- ============================================================
-- 2. TABLE_SIGNALETIQUE — Profil socio-démographique
-- ============================================================

------------------------------------------------------------------------------------------------

SELECT COUNT(DISTINCT TIERS_CLIENT) AS nb_clients_uniques
FROM raw.table_signaletique;

------------------------------------------------------------------------------------------------

SELECT
    MIN(AGE_CLIENT::numeric)    AS age_min,
    MAX(AGE_CLIENT::numeric)    AS age_max,
    ROUND(AVG(AGE_CLIENT::numeric), 2) AS age_moyen,
    ROUND(STDDEV(AGE_CLIENT::numeric), 2) AS age_ecart_type,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY AGE_CLIENT::numeric) AS age_Q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY AGE_CLIENT::numeric) AS age_mediane,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY AGE_CLIENT::numeric) AS age_Q3,
    COUNT(*) FILTER (WHERE AGE_CLIENT IS NULL) AS nb_null_age
FROM raw.table_signaletique;

------------------------------------------------------------------------------------------------
SELECT
    MIN(REVENU_MENSUEL::numeric)    AS revenu_min,
    MAX(REVENU_MENSUEL::numeric)    AS revenu_max,
    ROUND(AVG(REVENU_MENSUEL::numeric), 2)    AS revenu_moyen,
    ROUND(STDDEV(REVENU_MENSUEL::numeric), 2) AS revenu_ecart_type,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY REVENU_MENSUEL::numeric) AS revenu_Q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY REVENU_MENSUEL::numeric) AS revenu_median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY REVENU_MENSUEL::numeric) AS revenu_Q3,
    COUNT(*) FILTER (WHERE REVENU_MENSUEL IS NULL) AS nb_null_revenu
FROM raw.table_signaletique;

------------------------------------------------------------------------------------------------
SELECT
    ROUND(AVG(CHARGES_CLIE::numeric), 2)       AS charges_moyennes,
    ROUND(AVG(MENSUALITE_LOYER::numeric), 2)   AS loyer_moyen,
    COUNT(*) FILTER (WHERE CHARGES_CLIE IS NULL)     AS null_charges,
    COUNT(*) FILTER (WHERE MENSUALITE_LOYER IS NULL) AS null_loyer
FROM raw.table_signaletique;

------------------------------------------------------------------------------------------------
SELECT
    CIVILITE_CLIENT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY CIVILITE_CLIENT
ORDER BY nb_clients DESC;

------------------------------------------------------------------------------------------------
SELECT
    TYPE_CLIENT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY TYPE_CLIENT
ORDER BY nb_clients DESC;

------------------------------------------------------------------------------------------------
SELECT
    CSP_MKT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY CSP_MKT
ORDER BY nb_clients DESC
LIMIT 10;

------------------------------------------------------------------------------------------------
SELECT
    SECTEUR_ACTIVITE,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY SECTEUR_ACTIVITE
ORDER BY nb_clients DESC
LIMIT 10;

------------------------------------------------------------------------------------------------
SELECT
    CANAL_ENT_RELATION,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY CANAL_ENT_RELATION
ORDER BY nb_clients DESC;

------------------------------------------------------------------------------------------------
SELECT
    NBR_ENFANT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY NBR_ENFANT
ORDER BY NBR_ENFANT;

------------------------------------------------------------------------------------------------
SELECT
    FLAG_ELIGIBLE_MD,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY FLAG_ELIGIBLE_MD;

------------------------------------------------------------------------------------------------
SELECT col, nb_null, nb_total,
       ROUND(nb_null * 100.0 / NULLIF(nb_total, 0), 2) AS taux_null_pct
FROM (
    SELECT 'AGE_CLIENT'        AS col, COUNT(*) FILTER (WHERE AGE_CLIENT IS NULL)        AS nb_null, COUNT(*) AS nb_total FROM raw.table_signaletique
    UNION ALL SELECT 'REVENU_MENSUEL',   COUNT(*) FILTER (WHERE REVENU_MENSUEL IS NULL),   COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'CHARGES_CLIE',     COUNT(*) FILTER (WHERE CHARGES_CLIE IS NULL),     COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'MENSUALITE_LOYER', COUNT(*) FILTER (WHERE MENSUALITE_LOYER IS NULL), COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'CSP_MKT',          COUNT(*) FILTER (WHERE CSP_MKT IS NULL),          COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'SECTEUR_ACTIVITE', COUNT(*) FILTER (WHERE SECTEUR_ACTIVITE IS NULL), COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'NBR_ENFANT',       COUNT(*) FILTER (WHERE NBR_ENFANT IS NULL),       COUNT(*) FROM raw.table_signaletique
    UNION ALL SELECT 'FLAG_ELIGIBLE_MD', COUNT(*) FILTER (WHERE FLAG_ELIGIBLE_MD IS NULL), COUNT(*) FROM raw.table_signaletique
) t
ORDER BY taux_null_pct DESC;
------------------------------------------------------------------------------------------------
SELECT
    TYPE_CLIENT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY TYPE_CLIENT
ORDER BY nb_clients DESC;
------------------------------------------------------------------------------------------------
SELECT
    PREM_PRODUIT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY PREM_PRODUIT
ORDER BY nb_clients DESC
LIMIT 15;
-----------------------------------------------------------------------------------------------
SELECT
    ROUND(AVG(
        EXTRACT(YEAR FROM AGE(
            CURRENT_DATE,
            TO_TIMESTAMP(NULLIF(TRIM(DATE_ENT_RELATION), ''), 'DDMONYYYY:HH24:MI:SS')
        ))
    ), 2)                                                            AS anciennete_moy_annees,
    MIN(TO_TIMESTAMP(NULLIF(TRIM(DATE_ENT_RELATION), ''), 'DDMONYYYY:HH24:MI:SS')) AS date_min,
    MAX(TO_TIMESTAMP(NULLIF(TRIM(DATE_ENT_RELATION), ''), 'DDMONYYYY:HH24:MI:SS')) AS date_max,
    COUNT(*) FILTER (WHERE DATE_ENT_RELATION IS NULL)                AS nb_null
FROM raw.table_signaletique;
------------------------------------------------------------------------------------------------
SELECT
    DERNIER_EVT,
    COUNT(*) AS nb_clients,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_signaletique
GROUP BY DERNIER_EVT
ORDER BY nb_clients DESC
LIMIT 15;

-- ============================================================
-- 3. TABLE_RECLAMATION — Historique des crédits
-- ============================================================
SELECT
     COUNT(*)     AS nbr_ligne_total,
	 COUNT(DISTINCT ID_TIERS_SIEBEL	) AS nb_clients_uniques
FROM raw.table_reclamation;
------------------------------------------------------------------------------------------------
SELECT
      STATUT_DEMANDE,
	  COUNT(*) nb_reclamations,
	  ROUND(COUNT(*) * 100.0/SUM(COUNT(*)) OVER(),2) AS pct 
FROM raw.table_reclamation
GROUP BY STATUT_DEMANDE
ORDER BY nb_reclamations DESC ;
-------------------------------------------------------------------------------------------------
SELECT
      CATEGORIE,
	  COUNT(*) nb_reclamations,
	  ROUND(COUNT(*) * 100.0 /SUM(COUNT(*)) OVER (),2) AS pct
FROM raw.table_reclamation
GROUP BY CATEGORIE
ORDER BY nb_reclamations DESC
LIMIT 10;	
--------------------------------------------------------------------------------------------------
SELECT 
      CANAL,
	  COUNT(*) AS nb_reclamations,
	  ROUND(COUNT(*) * 100.0/ SUM(COUNT(*)) OVER (),2) AS pct
FROM raw.table_reclamation
GROUP BY CANAL
ORDER BY nb_reclamations DESC;
--------------------------------------------------------------------------------------------------
SELECT
    SOUS_CATEGORIE,
    COUNT(*) AS nb_reclamations,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_reclamation
GROUP BY SOUS_CATEGORIE
ORDER BY nb_reclamations DESC
LIMIT 10;
--------------------------------------------------------------------------------------------------
---------------------------table_demande_info-----------------------------------------------------
SELECT
    COUNT(*)                        AS nb_lignes_total,
    COUNT(DISTINCT ID_TIERS_SIEBEL) AS nb_clients_uniques
FROM raw.table_demande_info;
---------------------------------------------------------------------------------------------------
SELECT
    CATEGORIE,
    COUNT(*) AS nb_demandes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_demande_info
GROUP BY CATEGORIE
ORDER BY nb_demandes DESC
LIMIT 10;
---------------------------------------------------------------------------------------------------
SELECT
    SOUS_CATEGORIE,
    COUNT(*) AS nb_demandes,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_demande_info
GROUP BY SOUS_CATEGORIE
ORDER BY nb_demandes DESC
LIMIT 10;
----------------------------Table_sav--------------------------------------------------------------
SELECT
    COUNT(*)                        AS nb_lignes_total,
    COUNT(DISTINCT ID_TIERS_SIEBEL) AS nb_clients_uniques
FROM raw.table_sav;
----------------------------------------------------------------------------------------------------
SELECT
    CATEGORIE,
    COUNT(*) AS nb_sav,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_sav
GROUP BY CATEGORIE
ORDER BY nb_sav DESC
LIMIT 10;
----------------------------------------------------------------------------------------------------
SELECT
    AGENCE_CREATION,
    COUNT(*) AS nb_sav,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_sav
GROUP BY AGENCE_CREATION
ORDER BY nb_sav DESC
LIMIT 10;
----------------------------------------------------------------------------------------------------
---------------------------------Table_ciblage------------------------------------------------------
SELECT
    COUNT(*)              AS nb_lignes_total,
    COUNT(DISTINCT ID_TIER) AS nb_clients_uniques
FROM raw.table_ciblage;
----------------------------------------------------------------------------------------------------
SELECT
    ROUND(AVG(NB_SMS_RECU_J::numeric), 2)              AS moy_sms_recus,
    ROUND(AVG(NB_SMS_FAILED_J::numeric), 2)            AS moy_sms_echoues,
    ROUND(AVG(NB_APPELS_RECU_TMK_J::numeric), 2)       AS moy_appels_tmk,
    ROUND(AVG(NB_APPELS_FAILED_TMK_J::numeric), 2)     AS moy_appels_echoues,
    ROUND(AVG(NB_APPELS_RECU_MSGVOCALE_J::numeric), 2) AS moy_msg_vocaux,
    SUM(NB_SMS_RECU_J::numeric)                        AS total_sms,
    SUM(NB_APPELS_RECU_TMK_J::numeric)                 AS total_appels_tmk
FROM raw.table_ciblage;
----------------------------------------------------------------------------------------------------
SELECT
    COUNT(*) FILTER (WHERE NB_APPELS_RECU_TMK_J IS NULL)
        AS null_tmk,
    COUNT(*) FILTER (WHERE NB_APPELS_RECU_TMK_J IS NOT NULL)
        AS non_null_tmk,
    COUNT(*) FILTER (WHERE NB_SMS_RECU_J IS NULL)
        AS null_sms,
    COUNT(*) FILTER (WHERE NB_APPELS_RECU_MSGVOCALE_J IS NULL)
        AS null_msgvocal
FROM raw.table_ciblage;
----------------------------------------------------------------------------------------------------
SELECT
    RTC,
    COUNT(*) AS nb,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_ciblage
GROUP BY RTC
ORDER BY nb DESC;
-----------------------------------------------------------------------------------------------------
SELECT
    FLAG,
    COUNT(*) AS nb,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw.table_ciblage
GROUP BY FLAG
ORDER BY nb DESC;
-----------------------------------------------------------------------------------------------------
SELECT
    COUNT(*)                       AS nb_lignes_total,
    COUNT(DISTINCT TIERS_CLIENT)   AS nb_clients_uniques,
    COUNT(DISTINCT IE_AFFAIRE)     AS nb_credits_uniques
FROM raw.table_affaire;
------------------------------------------------------------------------------------------------------
-------------------------------------------------------TABLE_AFFAIRE----------------------------------
SELECT
    PRODUIT_WFS,
    COUNT(DISTINCT IE_AFFAIRE) AS nb_credits,
    ROUND(COUNT(DISTINCT IE_AFFAIRE) * 100.0 /
          SUM(COUNT(DISTINCT IE_AFFAIRE)) OVER (), 2) AS pct
FROM raw.table_affaire
GROUP BY PRODUIT_WFS
ORDER BY nb_credits DESC
LIMIT 10;
------------------------------------------------------------------------------------------------------
SELECT
    ROUND(AVG(MT_INIT_BRUT::numeric), 2)    AS moy_montant_init,
    ROUND(AVG(MT_CAP_REST::numeric), 2)     AS moy_capital_rest,
    ROUND(AVG(TAUX_CREDIT::numeric), 2)     AS moy_taux_credit,
    ROUND(AVG(MENSUALITE::numeric), 2)      AS moy_mensualite,
    ROUND(AVG(DUREE_INITIALE::numeric), 2)  AS moy_duree_mois,
    ROUND(AVG(NB_IMPAYE::numeric), 2)       AS moy_nb_impayes,
    ROUND(AVG(SOLDE_IMPAYE::numeric), 2)    AS moy_solde_impaye
FROM raw.table_affaire;
-------------------------------------------------------------------------------------------------------
SELECT
    COUNT(*)  FILTER (WHERE NB_IMPAYE::numeric = 0)  AS nb_sans_impaye,
    COUNT(*)  FILTER (WHERE NB_IMPAYE::numeric > 0)  AS nb_avec_impaye,
    COUNT(*)  FILTER (WHERE NB_IMPAYE::numeric >= 3) AS nb_impaye_grave,
    ROUND(COUNT(*) FILTER (WHERE NB_IMPAYE::numeric > 0) * 100.0
          / COUNT(*), 2)                             AS pct_avec_impaye,
    MAX(NB_IMPAYE::numeric)                          AS max_impayes,
    MAX(SOLDE_IMPAYE::numeric)                       AS max_solde_impaye
FROM raw.table_affaire;
-------------------------------------------------------------------------------------------------------
SELECT
    ROUND(AVG(nb_credits), 2)          AS moy_credits_par_client,
    MAX(nb_credits)                    AS max_credits_par_client,
    ROUND(AVG(montant_total), 2)       AS moy_montant_total,
    ROUND(AVG(nb_impayes_total), 2)    AS moy_impayes_client,
    COUNT(*) FILTER
        (WHERE nb_impayes_total > 0)   AS nb_clients_avec_impaye
FROM (
    SELECT
        TIERS_CLIENT,
        COUNT(DISTINCT IE_AFFAIRE)     AS nb_credits,
        SUM(MT_INIT_BRUT::numeric)     AS montant_total,
        SUM(NB_IMPAYE::numeric)        AS nb_impayes_total
    FROM raw.table_affaire
    GROUP BY TIERS_CLIENT
) t;