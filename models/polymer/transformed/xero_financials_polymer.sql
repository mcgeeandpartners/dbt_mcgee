{% set dict_data={'polymer': ['Clients', 'Division'],'reactivate': ['Clients', 'Division']} %}
{%  set sp='sp_polymer' %}

{{ xero_financials_common(dict_data,sp) }}