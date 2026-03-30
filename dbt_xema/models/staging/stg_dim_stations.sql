{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('xema_raw', 'raw_dim_stations') }}
)

select
    -- primary key
    cast(codi_estacio    as string)  as station_code,

    -- descriptive attributes
    cast(nom_estacio     as string)  as station_name,
    cast(codi_tipus      as string)  as station_type_code,
    cast(emplacament     as string)  as location_description,

    -- geographic coordinates
    cast(latitud         as float64) as latitude,
    cast(longitud        as float64) as longitude,
    cast(altitud         as float64) as altitude_m,

    -- administrative divisions
    cast(codi_municipi   as string)  as municipality_code,
    cast(nom_municipi    as string)  as municipality_name,
    cast(codi_comarca    as int64)   as comarca_code,
    cast(nom_comarca     as string)  as comarca_name,
    cast(codi_provincia  as int64)   as province_code,
    cast(nom_provincia   as string)  as province_name,

    -- network
    cast(codi_xarxa      as string)  as network_code,
    cast(nom_xarxa       as string)  as network_name,

    -- operational status
    cast(codi_estat_ema  as string)  as status_code,
    cast(nom_estat_ema   as string)  as status_name,
    cast(data_inici      as date)    as operational_start_date,
    cast(data_fi         as date)    as operational_end_date

from raw_data
where codi_estacio is not null