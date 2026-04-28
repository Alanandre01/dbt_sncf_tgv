with services as (
    select 'National'      as service
    union all
    select 'International' as service
)

select
    {{ dbt_utils.generate_surrogate_key(['service']) }}
        as service_sk,
    service

from services
