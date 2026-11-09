{#
  Deterministic surrogate key: md5 of the natural-key columns.
  NULLs get a sentinel so (NULL, 'x') and ('', 'x') don't collide and
  "unknown" combinations still get a stable key.
#}
{% macro surrogate_key(columns) -%}
    md5(concat_ws('|'
    {%- for col in columns %}, coalesce(cast({{ col }} as varchar), '__null__'){% endfor -%}
    ))
{%- endmacro %}
