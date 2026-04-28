with liaisons as (
    select * from {{ ref('stg_liaisons') }}
),

gares as (
    select * from {{ ref('stg_gares') }}
),

departements as (
    select * from {{ ref('stg_departements') }}
),

mapping as (
    select * from {{ ref('gares_mapping') }}
)

select
    -- clés de la liaison
    l.date_mois,
    l.service,
    l.gare_depart_norm   as gare_depart_key,
    l.gare_arrivee_norm  as gare_arrivee_key,

    -- infos gare départ (role = départ)
    g_dep.nom_gare            as nom_gare_depart,
    g_dep.trigramme           as trigramme_depart,
    g_dep.latitude            as latitude_depart,
    g_dep.longitude           as longitude_depart,
    g_dep.code_departement    as code_dept_depart,
    d_dep.nom_region          as region_depart,

    -- infos gare arrivée (role = arrivée)
    g_arr.nom_gare            as nom_gare_arrivee,
    g_arr.trigramme           as trigramme_arrivee,
    g_arr.latitude            as latitude_arrivee,
    g_arr.longitude           as longitude_arrivee,
    g_arr.code_departement    as code_dept_arrivee,
    d_arr.nom_region          as region_arrivee,

    -- métriques circulation
    l.duree_trajet_min,
    l.nb_circulations_prevues,
    l.nb_trains_annules,

    -- métriques retard arrivée (angle analytique principal)
    l.nb_trains_retard_arrivee,
    l.retard_moy_tous_arrivee_min,
    l.nb_trains_retard_15min,
    l.nb_trains_retard_30min,
    l.nb_trains_retard_60min,

    -- causes de retard
    l.prct_retard_causes_externes,
    l.prct_retard_infrastructure,
    l.prct_retard_gestion_trafic,
    l.prct_retard_materiel_roulant,
    l.prct_retard_gestion_gare,
    l.prct_retard_prise_en_charge_voyageurs

from liaisons l

-- join 1 : mapping gare de DÉPART (nom abrégé → nom officiel)
left join mapping m_dep
    on l.gare_depart_norm = m_dep.nom_liaison
-- join 2 : enrichissement gare départ (role-playing dimension)
left join gares g_dep
    on m_dep.nom_gare_officiel = g_dep.nom_gare

-- join 3 : mapping gare d'ARRIVÉE (même seed, alias différent)
left join mapping m_arr
    on l.gare_arrivee_norm = m_arr.nom_liaison
-- join 4 : enrichissement gare arrivée (role-playing dimension)
left join gares g_arr
    on m_arr.nom_gare_officiel = g_arr.nom_gare

-- join 5 : région de la gare départ
left join departements d_dep
    on g_dep.code_departement = d_dep.code_departement

-- join 6 : région de la gare arrivée
left join departements d_arr
    on g_arr.code_departement = d_arr.code_departement
