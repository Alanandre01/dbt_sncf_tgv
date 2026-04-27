with source as (
    select * from {{ source('sncf_raw', 'gares_voyageurs') }}
),

cleaned as (
    select
        -- identifiants
        "Nom_Gare"                              as nom_gare,
        "Trigramme"                             as trigramme,
        "Id_Gare"                               as id_gare,

        -- segment DRG : 9 grandes gares ont des valeurs multiples ("A;A", "A;B", "B;A;A")
        -- on prend le premier segment déclaré
        split_part("Segment(s) DRG", ';', 1)   as segment_drg,

        -- code UIC : 11 gares ont plusieurs codes séparés par ";"
        -- on garde la colonne brute pour traçabilité
        "Code_UIC"                              as code_uic_raw,
        split_part("Code_UIC", ';', 1)          as code_uic,

        -- coordonnées GPS : "49.6852237, 1.7743058" → FLOAT
        -- séparateur : virgule + espace ", "
        try_cast(
            split_part("Position géographique", ', ', 1)
        as double)                              as latitude,
        try_cast(
            split_part("Position géographique", ', ', 2)
        as double)                              as longitude,

        -- code département extrait du code INSEE commune (toujours 5 caractères)
        -- DOM (971–976) : les 2 premiers chars valent "97" → prendre 3 chars
        case
            when left("Code commune", 2) = '97'
            then left("Code commune", 3)
            else left("Code commune", 2)
        end                                     as code_departement,

        "Code commune"                          as code_commune,

        -- nom normalisé pour la jointure avec stg_liaisons
        -- note : un seed gares_mapping sera nécessaire car les noms de liaisons
        -- sont en UPPER abrégé ("BORDEAUX ST JEAN" vs "Bordeaux Saint-Jean")
        upper(trim("Nom_Gare"))                 as nom_gare_norm

    from source
)

select * from cleaned
