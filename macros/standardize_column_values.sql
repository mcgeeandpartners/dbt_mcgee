--macro to be main used for ghseet connector files.

{% macro standardize_column_values(column_name) -%}

nullif(lower(trim({{ column_name }})), '0') as {{ column_name }}

{%- endmacro %}