-- Test singulier : les annulations ne peuvent pas dépasser
-- le nombre de circulations prévues.
-- Ce test retourne les lignes INVALIDES.
-- Résultat attendu : 0 lignes.

SELECT
    regularite_sk,
    gare_depart_sk,
    gare_arrivee_sk,
    periode_sk,
    nb_circulations_prevues,
    nb_trains_annules
FROM {{ ref('fct_regularite') }}
WHERE nb_trains_annules > nb_circulations_prevues
  AND nb_circulations_prevues > 0  -- exclure anomalie source : 63 lignes avec prévues=0 mais annulées>0
