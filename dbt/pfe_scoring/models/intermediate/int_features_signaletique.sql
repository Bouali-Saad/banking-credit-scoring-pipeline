

WITH source AS (
    SELECT * FROM {{ ref('stg_signaletique') }}
),

deduped AS (
    SELECT DISTINCT ON (tiers_client, periode_trt)
        tiers_client,
        id_tiers_siebel,
        periode_trt,

        
        age,
        revenu,
        nbr_enfant,
        charges,
        mensualite_loyer,

        
        csp_mkt,
        secteur_activite,
        civilite_client,
        prem_produit,
        canal_ent_relation,
        type_client,
        activite_profession,
        dernier_evt,

        
        EXTRACT(YEAR FROM AGE(CURRENT_DATE,
            TO_TIMESTAMP(
                NULLIF(TRIM(date_entree::text), ''),
                'DDMONYYYY:HH24:MI:SS'
            )
        ))::numeric                         AS anciennete_annees,

        
        EXTRACT(YEAR FROM AGE(CURRENT_DATE,
            TO_TIMESTAMP(
                NULLIF(TRIM(date_embauche::text), ''),
                'DDMONYYYY:HH24:MI:SS'
            )
        ))::numeric                         AS anciennete_emploi,

        
        EXTRACT(DAY FROM CURRENT_DATE -
            TO_TIMESTAMP(
                NULLIF(TRIM(date_dernier_evt::text), ''),
                'DDMONYYYY:HH24:MI:SS'
            )
        )::numeric                          AS nb_jours_dernier_evt,

        
        EXTRACT(DAY FROM
            TO_TIMESTAMP(
                NULLIF(TRIM(date_trt_extr::text), ''),
                'DDMONYYYY:HH24:MI:SS') -
            TO_TIMESTAMP(
                NULLIF(TRIM(date_entree::text), ''),
                'DDMONYYYY:HH24:MI:SS')
        )::numeric                          AS delai_extraction

        

    FROM source
    WHERE tiers_client IS NOT NULL
    ORDER BY tiers_client,
             periode_trt,
             date_entree DESC NULLS LAST
)

SELECT * FROM deduped