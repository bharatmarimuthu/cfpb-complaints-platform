{#
  Normalize a text field from the raw JSON: trim whitespace and turn the
  placeholder values the CFPB data uses ('', 'None', 'N/A') into real NULLs.
#}
{% macro clean_text(expr) -%}
    nullif(nullif(nullif(trim({{ expr }}), ''), 'None'), 'N/A')
{%- endmacro %}
