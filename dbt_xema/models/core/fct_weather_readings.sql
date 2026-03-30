{{ config(
    materialized='incremental',
    unique_key='record_id',
    partition_by={
      "field": "reading_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['station_code', 'variable_code']
) }}

with staging as (

    select * from {{ ref('stg_weather_data') }}

    {% if is_incremental() %}
      where reading_date >= (
          select coalesce(max(reading_date), '1970-01-01')
          from {{ this }}
      )
    {% endif %}

),

dim_variables as (
    select * from {{ ref('stg_dim_variables') }}
),

dim_stations as (
    select * from {{ ref('stg_dim_stations') }}
)

select
    -- Keys
    stg.record_id,
    stg.station_code,
    stg.variable_code,

    -- Temporal: date for partitioning, full timestamp for analytics
    stg.reading_date,
    stg.reading_timestamp,

    -- Station context
    sta.station_name,
    sta.municipality_name,
    sta.comarca_name,
    sta.latitude,
    sta.longitude,
    sta.altitude_m,

    -- Variable context
    var.variable_name,
    var.unit,
    var.acronym,

    -- Measurement
    stg.reading_value,
    stg.status_code

from staging             as stg
left join dim_stations   as sta using (station_code)
left join dim_variables  as var using (variable_code)