# dbt_sncf_tgv

Projet dbt d'analyse de la ponctualité des TGV français à partir des données open data SNCF.

## Stack

- **dbt** 1.11.8
- **DuckDB** — base de données locale (`sncf.duckdb`)
- **Python** — chargement des sources brutes

## Sources de données

| Fichier | Description | Séparateur |
|---|---|---|
| `data/raw/regularite_liaisons.csv` | Ponctualité mensuelle TGV par liaison | `;` |
| `data/raw/gares_voyageurs.csv` | Référentiel des gares françaises avec coordonnées GPS | `;` |
| `seeds/departements.csv` | Mapping départements → régions (102 lignes) | `,` |

## Structure du projet

```
models/
  staging/       # nettoyage et renommage des sources brutes (vues)
  intermediate/  # transformations intermédiaires (vues)
  marts/         # tables analytiques finales (tables)
seeds/
  departements.csv
data/
  raw/           # CSV bruts non gérés par dbt
scripts/
  load_sources.py  # charge les CSV dans DuckDB sous le schéma raw
macros/
  safe_cast.sql    # TRY_CAST — retourne NULL si le cast échoue
```

## Mise en place

### 1. Installer les dépendances

```bash
pip install -r requirements.txt
dbt deps
```

### 2. Charger les sources brutes dans DuckDB

```bash
python scripts/load_sources.py
```

### 3. Charger les seeds

```bash
dbt seed
```

### 4. Lancer les modèles

```bash
dbt run
```

### 5. Lancer les tests

```bash
dbt test
```

## Profil dbt

Le projet utilise le profil `dbt_sncf_tgv` défini dans `~/.dbt/profiles.yml`.
