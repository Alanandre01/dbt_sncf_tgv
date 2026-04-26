import duckdb

conn = duckdb.connect('sncf.duckdb')

# Créer un schema pour les sources brutes
conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

# Charger regularite_liaisons
conn.execute("""
    CREATE OR REPLACE TABLE raw.regularite_liaisons AS
    SELECT * FROM read_csv_auto(
        'data/raw/regularite_liaisons.csv',
        delim=';',
        header=True
    )
""")

# Charger gares_voyageurs
conn.execute("""
    CREATE OR REPLACE TABLE raw.gares_voyageurs AS
    SELECT * FROM read_csv_auto(
        'data/raw/gares_voyageurs.csv',
        delim=';',
        header=True
    )
""")

conn.close()
print("Sources chargées avec succès")
