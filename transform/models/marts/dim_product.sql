select
    {{ surrogate_key(['product', 'sub_product']) }} as product_key,
    coalesce(product, 'Unknown')                     as product,
    coalesce(sub_product, '(none)')                  as sub_product
from {{ ref('stg_complaints') }}
group by product, sub_product
