select npi, provider_name, role, updated_at
from (
    select *,
           row_number() over (
               partition by npi
               order by case when role = 'doctor' then 0 else 1 end, updated_at desc
           ) as rn
    from {{ ref('raw_providers') }}
) as ranked
where rn = 1