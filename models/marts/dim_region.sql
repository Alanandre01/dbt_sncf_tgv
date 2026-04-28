with regions as (
    select distinct
        nom_region
    from {{ ref('stg_departements') }}
    where nom_region is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['nom_region']) }}
        as region_sk,
    nom_region

from regions
order by nom_region
