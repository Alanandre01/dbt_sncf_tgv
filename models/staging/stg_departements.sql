with source as (
    select * from {{ ref('departements') }}
)

select
    trim(num_dep)      as code_departement,
    trim(dep_name)     as nom_departement,
    trim(region_name)  as nom_region
from source
where num_dep is not null
