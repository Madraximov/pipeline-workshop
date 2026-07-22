with source as (
    select * from {{ source('raw', 'customers') }}
),

deduped as (
    select
        id            as customer_id,
        full_name,
        lower(email)  as email,          -- AZIZA@... -> aziza@...
        upper(country) as country_code,  -- uz -> UZ
        created_at,
        -- дубль клиента 4: нумеруем и оставляем только первую строку
        row_number() over (partition by id order by created_at) as rn
    from source
)

select customer_id, full_name, email, country_code, created_at
from deduped
where rn = 1