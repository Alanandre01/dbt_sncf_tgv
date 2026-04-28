-- Test singulier : la somme des 6 colonnes de causes
-- doit être proche de 100% (tolérance ±1 pour les arrondis SNCF).
-- On ignore les lignes sans données de causes.
-- Résultat attendu : 0 lignes.

WITH causes AS (
    SELECT
        regularite_sk,
        periode_sk,
        COALESCE(prct_retard_causes_externes, 0)
        + COALESCE(prct_retard_infrastructure, 0)
        + COALESCE(prct_retard_gestion_trafic, 0)
        + COALESCE(prct_retard_materiel_roulant, 0)
        + COALESCE(prct_retard_gestion_gare, 0)
        + COALESCE(prct_retard_prise_en_charge_voyageurs, 0) AS somme_causes
    FROM {{ ref('fct_regularite') }}
)

SELECT
    regularite_sk,
    periode_sk,
    somme_causes
FROM causes
WHERE
    somme_causes > 0              -- exclure les lignes sans causes
    AND ABS(somme_causes - 100) > 1  -- tolérance ±1%
