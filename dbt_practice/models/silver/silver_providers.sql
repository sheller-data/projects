{{ config(
    materialized='incremental',
    unique_key='npi',
    incremental_strategy='merge'
) }}

select * from {{ ref('stg_providers') }}

{% if is_incremental() %}
where updated_at > (select max(updated_at) - interval 2 hours from {{ this }})
{% endif %}