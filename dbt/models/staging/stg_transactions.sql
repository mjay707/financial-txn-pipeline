with source as (
    select * from {{ source('silver', 'transactions') }}
),

renamed as (
    select
        transaction_id,
        customer_id,
        customer_name,
        account_number,
        branch_id,
        region,
        amount,
        transaction_type,
        event_time,
        received_time,
        latency_seconds,
        record_status
    from source
)

select * from renamed