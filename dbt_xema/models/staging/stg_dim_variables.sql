{{ config(materialized='view') }}

with raw_data as (
    select * from {{ source('xema_raw', 'raw_dim_variables') }}
)

select
    -- primary key
    cast(codi_variable   as string)  as variable_code,

    -- descriptive attributes
    cast(nom_variable    as string)  as variable_name,
    cast(unitat          as string)  as unit,
    cast(acronim         as string)  as acronym,
    cast(codi_tipus_var  as string)  as variable_type_code,
    cast(decimals        as int64)   as decimal_places

from raw_data
where codi_variable is not null