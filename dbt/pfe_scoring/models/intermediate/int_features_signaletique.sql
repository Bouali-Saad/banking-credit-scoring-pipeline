

WITH source AS (
    SELECT * FROM {{ ref('stg_signaletique') }}
),

deduped AS (
    SELECT DISTINCT ON (tiers_client, period_trt)
        tiers_client,
        id_tiers_siebel,
        period_trt,

        
        age,
        revenu,
        nbr_enfant,
        charges,
        mensualite_loyer,
        flag_eligible_md,

        
        csp_mkt,
        secteur_activite,
        civilite_client,
        prem_produit,
        canal_ent_relation,
        type_client,
        activite_profession,
        dernier_evt,
        type_prel_prioritaire,

        
        EXTRACT(YEAR FROM AGE(CURRENT_DATE,
            TO_TIMESTAMP(
                NULLIF(TRIM(date_entree::text), ''),
                'DDMONYYYY:HH24:MI:SS'
            )
        ))::numeric                     AS anciennete_annees,

        
        date_entree,
        date_dernier_evt,
        date_embauche

    FROM source
    WHERE tiers_client IS NOT NULL
    ORDER BY tiers_client,
             period_trt,
             date_entree DESC NULLS LAST
)

SELECT * FROM deduped