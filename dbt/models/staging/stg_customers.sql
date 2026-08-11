with source as (
    select * from {{ source('silver', 'transactions') }}
),

customers as (
    select distinct
        customer_id,
        customer_name,
        region
    from source
)

select * from customers