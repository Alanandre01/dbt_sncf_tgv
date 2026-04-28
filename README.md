# dbt_sncf_tgv

> Pipeline analytique de la ponctualité TGV en France
> **Stack :** dbt Core 1.11.8 · DuckDB · Git · data.sncf.com · data.gouv.fr

---

## Contexte

Ce projet modélise les données de régularité des TGV SNCF
pour analyser les retards, annulations et leurs causes
par liaison, région et période.

**Questions analytiques adressées :**
- Quelles liaisons TGV ont le taux d'annulation le plus élevé ?
- Quelle est la principale cause de retard par région ?
- Comment évolue la ponctualité par trimestre ?

**Sources :** 3 fichiers publics (data.sncf.com + data.gouv.fr)
**Périmètre :** 12 181 lignes de faits · 51 gares françaises · 130 liaisons · 99 mois (jan. 2018 → mars 2026)

---

## Architecture

| Couche | Modèle | Lignes | Description |
|---|---|---|---|
| Staging | `stg_liaisons` | 12 181 | Nettoyage source régularité (parsing dates, NULLIF COVID) |
| Staging | `stg_gares` | 2 782 | Nettoyage référentiel gares (lat/lon, code département) |
| Staging | `stg_departements` | 101 | Seed départements → régions |
| Intermediate | `int_liaisons_enrichies` | 12 181 | Jointure liaisons × gares × départements (6 jointures) |
| Marts | `fct_regularite` | 12 181 | Table de faits (grain : liaison × mois × service) |
| Marts | `dim_gare` | 2 782 | Dimension gares (role-playing) |
| Marts | `dim_region` | 18 | Dimension régions |
| Marts | `dim_service` | 2 | Dimension service (National / International) |
| Marts | `dim_periode` | 99 | Dimension temporelle |

---

## Structure du projet

```
models/
  staging/         # nettoyage et renommage des sources brutes (vues)
  intermediate/    # jointures enrichies (vues)
  marts/           # tables analytiques finales (tables)
seeds/
  departements.csv      # 101 départements → régions
  gares_mapping.csv     # correspondance noms SNCF ↔ noms officiels gares
data/
  raw/             # CSV bruts (non gérés par dbt)
scripts/
  load_sources.py  # charge les CSV bruts dans DuckDB (schéma raw)
macros/
  safe_cast.sql    # TRY_CAST — retourne NULL si le cast échoue
tests/
  assert_annulations_coherentes.sql
  assert_prct_causes_somme_100.sql
```

---

## Lancer le projet

```bash
# 1. Cloner le repo
git clone https://github.com/Alanandre01/dbt_sncf_tgv.git
cd dbt_sncf_tgv

# 2. Installer les dépendances Python
pip install -r requirements.txt

# 3. Installer les packages dbt
dbt deps

# 4. Charger les CSV bruts dans DuckDB (schéma raw)
python scripts/load_sources.py

# 5. Charger les seeds (départements + mapping gares)
dbt seed

# 6. Lancer les transformations
dbt run

# 7. Valider la qualité des données
dbt test

# 8. Explorer la documentation interactive
dbt docs generate && dbt docs serve
```

---

## Tests de qualité

46 tests — 46/46 PASS

| Couche | Tests | Résultat |
|---|---|---|
| Staging | 25 | 25/25 PASS |
| Marts (génériques) | 19 | 19/19 PASS |
| Tests singuliers | 2 | 2/2 PASS |

**Tests génériques :** `not_null` + `unique` sur toutes les surrogate keys,
`relationships` sur les 4 FK de `fct_regularite`, `accepted_values` sur service, trimestre et `cause_principale`.

**Tests singuliers :**
- `assert_annulations_coherentes` — `nb_annulés ≤ nb_prévus`
- `assert_prct_causes_somme_100` — somme des 6 causes ≈ 100 % (±1, hors lignes sans données)

---

## Décisions techniques

**Seed `gares_mapping` pour le matching des noms de gares**
Les fichiers source utilisent des casses incompatibles (`BORDEAUX ST JEAN` vs `Bordeaux Saint-Jean`).
56 % des gares ne matchent pas après `UPPER()`.
Solution : un seed de 59 lignes qui fait correspondance manuellement chaque nom source avec le nom officiel du référentiel.

**LEFT JOIN pour les gares étrangères**
Les liaisons internationales (Stuttgart, Barcelona, Zurich…) n'ont pas de correspondance dans le référentiel gares français.
Un LEFT JOIN produit des NULLs propres sur les colonnes gare/région (733 lignes, 8 gares) plutôt qu'une perte de lignes.

**Role-playing dimension**
`dim_gare` est référencée deux fois dans `fct_regularite` :
`gare_depart_sk` et `gare_arrivee_sk` pointent vers la même table.
Ce pattern évite de dupliquer la dimension gare.

**Surrogate key sur clés naturelles dans `fct_regularite`**
La SK est générée sur `(gare_depart_key, gare_arrivee_key, date_mois, service)` plutôt que sur les SKs dimension,
car les 733 lignes de gares étrangères ont `gare_sk = NULL`, ce qui causait 468 doublons avec `generate_surrogate_key`.

---

## Sources de données

| Fichier | Source | Description |
|---|---|---|
| `data/raw/regularite_liaisons.csv` | data.sncf.com | Régularité mensuelle TGV par liaison (sep. `;`) |
| `data/raw/gares_voyageurs.csv` | data.sncf.com | Référentiel gares françaises avec coordonnées GPS (sep. `;`) |
| `seeds/departements.csv` | data.gouv.fr | Mapping départements → régions (sep. `,`) |
