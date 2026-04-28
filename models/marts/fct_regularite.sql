{{
    config(
        materialized='table'
    )
}}

with liaisons as (
    select * from {{ ref('int_liaisons_enrichies') }}
),

dim_gare as (
    select gare_sk, nom_gare
    from {{ ref('dim_gare') }}
),

dim_service as (
    select service_sk, service
    from {{ ref('dim_service') }}
),

dim_periode as (
    select periode_sk, date_mois
    from {{ ref('dim_periode') }}
),

fct as (
    select
        -- Surrogate key basée sur les clés naturelles du grain (SKs dimension peuvent être NULL pour gares étrangères)
        {{ dbt_utils.generate_surrogate_key([
            'l.gare_depart_key',
            'l.gare_arrivee_key',
            'l.date_mois',
            'l.service'
        ]) }} as regularite_sk,

        -- Clés étrangères
        g_dep.gare_sk   as gare_depart_sk,
        g_arr.gare_sk   as gare_arrivee_sk,
        p.periode_sk,
        sv.service_sk,

        -- Métriques circulation
        l.nb_circulations_prevues,
        l.nb_trains_annules,
        round(
            l.nb_trains_annules::float
            / nullif(l.nb_circulations_prevues, 0),
            4
        )               as taux_annulation,
        l.duree_trajet_min,

        -- Métriques retard arrivée
        l.nb_trains_retard_arrivee,
        l.retard_moy_tous_arrivee_min,
        l.nb_trains_retard_15min,
        l.nb_trains_retard_30min,
        l.nb_trains_retard_60min,

        -- Causes de retard (%)
        l.prct_retard_causes_externes,
        l.prct_retard_infrastructure,
        l.prct_retard_gestion_trafic,
        l.prct_retard_materiel_roulant,
        l.prct_retard_gestion_gare,
        l.prct_retard_prise_en_charge_voyageurs,

        -- Cause principale de retard
        case
            when l.prct_retard_causes_externes = greatest(
                l.prct_retard_causes_externes, l.prct_retard_infrastructure,
                l.prct_retard_gestion_trafic, l.prct_retard_materiel_roulant,
                l.prct_retard_gestion_gare, l.prct_retard_prise_en_charge_voyageurs
            ) then 'externe'
            when l.prct_retard_infrastructure = greatest(
                l.prct_retard_causes_externes, l.prct_retard_infrastructure,
                l.prct_retard_gestion_trafic, l.prct_retard_materiel_roulant,
                l.prct_retard_gestion_gare, l.prct_retard_prise_en_charge_voyageurs
            ) then 'infra'
            when l.prct_retard_gestion_trafic = greatest(
                l.prct_retard_causes_externes, l.prct_retard_infrastructure,
                l.prct_retard_gestion_trafic, l.prct_retard_materiel_roulant,
                l.prct_retard_gestion_gare, l.prct_retard_prise_en_charge_voyageurs
            ) then 'gestion_trafic'
            when l.prct_retard_materiel_roulant = greatest(
                l.prct_retard_causes_externes, l.prct_retard_infrastructure,
                l.prct_retard_gestion_trafic, l.prct_retard_materiel_roulant,
                l.prct_retard_gestion_gare, l.prct_retard_prise_en_charge_voyageurs
            ) then 'materiel'
            when l.prct_retard_gestion_gare = greatest(
                l.prct_retard_causes_externes, l.prct_retard_infrastructure,
                l.prct_retard_gestion_trafic, l.prct_retard_materiel_roulant,
                l.prct_retard_gestion_gare, l.prct_retard_prise_en_charge_voyageurs
            ) then 'gare'
            else 'voyageurs'
        end             as cause_principale

    from liaisons l

    -- Role-playing dimension : dim_gare jointe 2 fois
    left join dim_gare g_dep
        on l.nom_gare_depart = g_dep.nom_gare
    left join dim_gare g_arr
        on l.nom_gare_arrivee = g_arr.nom_gare

    left join dim_service sv
        on l.service = sv.service

    left join dim_periode p
        on l.date_mois = p.date_mois
)

select * from fct
