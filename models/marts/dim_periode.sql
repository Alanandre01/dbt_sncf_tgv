with periodes as (
    select distinct
        date_mois
    from {{ ref('stg_liaisons') }}
    where date_mois is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['date_mois']) }}
        as periode_sk,

    date_mois,
    year(date_mois)                         as annee,
    month(date_mois)                        as mois_numero,
    quarter(date_mois)                      as trimestre,

    case month(date_mois)
        when 1  then 'Janvier'
        when 2  then 'Février'
        when 3  then 'Mars'
        when 4  then 'Avril'
        when 5  then 'Mai'
        when 6  then 'Juin'
        when 7  then 'Juillet'
        when 8  then 'Août'
        when 9  then 'Septembre'
        when 10 then 'Octobre'
        when 11 then 'Novembre'
        when 12 then 'Décembre'
    end                                     as libelle_mois

from periodes
order by date_mois
