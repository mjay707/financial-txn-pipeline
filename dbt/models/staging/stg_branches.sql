with source as (
    select * from {{ source('silver', 'transactions') }}
),

branches as (
    select distinct
        branch_id,
        region
    from source
)

select * from branches