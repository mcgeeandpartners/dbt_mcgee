
{% macro xero_financials_common(dict_data,sp) %}

{{ log(sp, info=True) }}
with  
{% for cte_name,cte_category in dict_data.items() %}
{{cte_name}}_combined as
   ( select
        jl.journal_line_id, 
        jl.account_id, 
        psp.account_code, --jl
        psp.account_type, --jl
        psp.account_name, --jl
        jl.net_amount as net_amount_original,  
        CASE 
            WHEN jl.account_type in ('REVENUE','OTHERINCOME') THEN jl.net_amount * -1  
            ELSE jl.net_amount * 1
        END AS net_amount,
        j.journal_id, 
        j.journal_date, 
        psp.account_category, 
        psp.account_report, 
        a.class as account_class,
        psp.working_capital as working_capital,
        '{{cte_name}}' as entity,
   {% for cc in cte_category %}
        CASE 
            WHEN tc.name = '{{cc}}' THEN tco.name 
            ELSE NULL 
        END AS {{cc.rstrip('s')}}

        {%if not loop.last %} , {% endif %}
        {% endfor %}

    from {{ source(cte_name, "journal") }} j
    left join {{ source(cte_name, "journal_line") }} jl on j.journal_id = jl.journal_id
    left join {{ source(cte_name, "journal_line_has_tracking_category") }} jlt
        on jl.journal_line_id = jlt.journal_line_id
        and jl.journal_id = jlt.journal_id
    left join {{source (cte_name,"account")}} a on jl.account_id=a.account_id

    left join {{ source(cte_name, "tracking_category_has_option") }} tcho
        on jlt.tracking_category_id = tcho.tracking_category_id
        and jlt.tracking_category_option_id = tcho.tracking_option_id
    left join {{ source(cte_name, "tracking_category_option") }} tco
        on tcho.tracking_option_id = tco.tracking_option_id
    left join {{ source(cte_name, "tracking_category") }} tc
        on tc.tracking_category_id = jlt.tracking_category_id
     
    left join {{ source(sp, "coa_metadata") }} psp
 
   
        on lower(psp.account_id) = lower(jl.account_id)
    where tc.name IN (
           {% for cc in cte_category %}
        '{{cc}}'
         
          {%if not loop.last %} , {% endif %}
        
        {% endfor %}
        )
    or tc.name is NULL 
)
{% if not loop.last %} , {% endif %}

{% endfor %}

{% for cte_name,v in dict_data.items() %}

select {{cte_name}}_combined.*, dd.fiscal_year, dd.fiscal_quarter,dd.yearmonth from {{cte_name}}_combined

left join {{ ref("dim_date") }} dd on {{cte_name}}_combined.journal_date::date = dd.date

{% if not loop.last %}
union all
{% endif %}
{% endfor %}
{% endmacro %}
