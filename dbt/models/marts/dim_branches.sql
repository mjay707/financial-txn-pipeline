{{ config(materialized='table') }}

with branches as (
    select * from {{ ref('stg_branches') }}
)

select
    branch_id,
    region,
    current_timestamp as updated_at
from branches