with source as (
    select * from {{ source('sncf_raw', 'regularite_liaisons') }}
),

renamed as (
    select
        -- identifiants
        "Service"        as service,
        "Gare de départ" as gare_depart,
        "Gare d'arrivée" as gare_arrivee,

        -- date : "YYYY-MM" (VARCHAR) → DATE au 1er du mois
        cast("Date" || '-01' as date) as date_mois,

        -- circulation
        -- nullif sur la durée : 0 est physiquement impossible, apparaît sur lignes COVID sans service
        nullif(cast("Durée moyenne du trajet" as integer), 0)       as duree_trajet_min,
        cast("Nombre de circulations prévues" as integer)           as nb_circulations_prevues,
        cast("Nombre de trains annulés" as integer)                 as nb_trains_annules,

        -- retards au départ
        cast("Nombre de trains en retard au départ" as integer)     as nb_trains_retard_depart,
        "Retard moyen des trains en retard au départ"               as retard_moy_retardes_depart_min,
        "Retard moyen de tous les trains au départ"                 as retard_moy_tous_depart_min,

        -- retards à l'arrivée
        cast("Nombre de trains en retard à l'arrivée" as integer)   as nb_trains_retard_arrivee,
        "Retard moyen des trains en retard à l'arrivée"             as retard_moy_retardes_arrivee_min,
        "Retard moyen de tous les trains à l'arrivée"               as retard_moy_tous_arrivee_min,

        -- seuils (granularité fine pour analyses de queues de distribution)
        cast("Nombre trains en retard > 15min" as integer)          as nb_trains_retard_15min,
        cast("Nombre trains en retard > 30min" as integer)          as nb_trains_retard_30min,
        cast("Nombre trains en retard > 60min" as integer)          as nb_trains_retard_60min,

        -- causes de retard : déjà en DOUBLE (0.0–100.0), somme ≈ 100 quand non-nulle
        -- 269 lignes ont toutes les causes à 0.0 simultanément (pas de données causes ce mois-là)
        "Prct retard pour causes externes"                                                            as prct_retard_causes_externes,
        "Prct retard pour cause infrastructure"                                                       as prct_retard_infrastructure,
        "Prct retard pour cause gestion trafic"                                                       as prct_retard_gestion_trafic,
        "Prct retard pour cause matériel roulant"                                                     as prct_retard_materiel_roulant,
        "Prct retard pour cause gestion en gare et réutilisation de matériel"                         as prct_retard_gestion_gare,
        "Prct retard pour cause prise en compte voyageurs (affluence, gestions PSH, correspondances)" as prct_retard_prise_en_charge_voyageurs,

        -- normalisation UPPER pour jointure future avec stg_gares
        -- attention : 33/59 gares ne matchent pas directement — une seed de correspondance sera nécessaire
        upper(trim("Gare de départ"))  as gare_depart_norm,
        upper(trim("Gare d'arrivée")) as gare_arrivee_norm

    from source
)

select * from renamed
