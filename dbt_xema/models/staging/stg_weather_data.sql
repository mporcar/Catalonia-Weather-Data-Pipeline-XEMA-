{{ config(materialized='view') }}
 
with raw_data as (
    select * from {{ source('xema_raw', 'raw_weather_data') }}
)
 
select
    -- identifiers
    cast(id as string)              as record_id,
    cast(codi_estacio as string)    as station_code,
    cast(codi_variable as string)   as variable_code,
 
    -- timestamps: keep full precision for analytics
    DATETIME(TIMESTAMP_MICROS(CAST(reading_timestamp/1000 AS INT64))) AS reading_datetime,
 
    -- date derived from timestamp — used for BQ partitioning
    date(cast(data_lectura as timestamp)) as reading_date,
 
    -- facts
    cast(valor_lectura as float64)  as reading_value,
 
    -- metadata
    cast(codi_estat as string)      as status_code,
    cast(codi_base as string)       as base_code
 
from raw_data
where id is not null
 