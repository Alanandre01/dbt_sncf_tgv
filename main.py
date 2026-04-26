import duckdb

con = duckdb.connect()

result = con.execute("""
    SELECT
        SIREN,
        SIRET,
        "Dénomination de l'unité légale"              AS denomination,
        "Date de création de l'établissement"         AS date_creation,
        "Etat administratif de l'établissement"       AS statut,
        "Activité principale de l'établissement"      AS code_naf,
        "Division de l'établissement"                 AS secteur
    FROM read_csv_auto('seeds/base-sirene-nantes.csv')
    WHERE "Date de création de l'établissement" IS NOT NULL
    AND "Etat administratif de l'établissement" = 'Actif'
    ORDER BY "Date de création de l'établissement" ASC
    LIMIT 10
""").df()

print(result)