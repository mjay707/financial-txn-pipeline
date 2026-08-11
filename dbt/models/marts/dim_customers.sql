{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    customer_name,
    region,
    current_timestamp as updated_at
from customers