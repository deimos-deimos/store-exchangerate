merge_procedure = '''
with ranked as (
        select 
            *,
            dense_rank() over (partition by pair, price_date order by tech_load_date desc) as dr
        from
            stage.prices_rate
), to_insert as (
    select
        pair,
        price_date,
        price
    from
        ranked
    where
        dr = 1
)
insert into model.prices_rate (pair, price_date, price)
select 
    pair, 
    price_date, 
    price
from 
    to_insert
on conflict (pair, price_date, price) do update 
set price = excluded.price;
'''
