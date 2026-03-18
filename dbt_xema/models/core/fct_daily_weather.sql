{{ config(
    materialized='incremental',
    unique_key='daily_station_var_id',
    partition_by={
      "field": "reading_date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

with staging as (
    select * from {{ ref('stg_weather_data') }}
    
    {% if is_incremental() %}
      -- this filter will only be applied on an incremental run
      -- we process any date greater than or equal to the maximum date we have currently
      where reading_date >= (select coalesce(max(reading_date), '1970-01-01') from {{ this }})
    {% endif %}
),

daily_aggregates as (
    select
        station_code,
        variable_code,
        reading_date,
        -- Generate a unique key for the daily aggregate
        concat(station_code, '_', variable_code, '_', cast(reading_date as string)) as daily_station_var_id,
        
        avg(reading_value) as avg_daily_value,
        max(reading_value) as max_daily_value,
        min(reading_value) as min_daily_value,
        count(record_id) as reading_count
        
    from staging
    group by 1, 2, 3, 4
)

select * from daily_aggregates
