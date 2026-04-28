with gares as (
    select * from {{ ref('stg_gares') }}
),

departements as (
    select * from {{ ref('stg_departements') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['g.nom_gare']) }}
        as gare_sk,

    g.nom_gare,
    g.nom_gare_norm,
    g.trigramme,
    g.segment_drg,
    g.code_uic,
    g.latitude,
    g.longitude,
    g.code_departement,
    d.nom_departement,
    d.nom_region

from gares g
left join departements d
    on g.code_departement = d.code_departement
