-- stg_air_quality
-- Responsabilidade: limpar, tipar e padronizar as medições brutas.
-- Regras aplicadas:
--   * remove registros sem valor de medição ou sem timestamp
--   * garante que `value` seja não-negativo (medições negativas são erro de sensor)
--   * deduplica por (location_id, parameter, measured_at_utc)

with source as (
    select * from {{ source('raw', 'air_quality_measurements') }}
),

cleaned as (
    select
        location_id,
        location_name,
        city_query                     as city,
        parameter,
        unit,
        cast(value as double)          as value,
        cast(measured_at_utc as timestamp) as measured_at_utc,
        cast(extracted_at as timestamp)    as extracted_at
    from source
    where value is not null
      and measured_at_utc is not null
      and value >= 0
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by location_id, parameter, measured_at_utc
            order by extracted_at desc
        ) as rn
    from cleaned
)

select
    location_id,
    location_name,
    city,
    parameter,
    unit,
    value,
    measured_at_utc,
    date(measured_at_utc) as measured_date
from deduplicated
where rn = 1
