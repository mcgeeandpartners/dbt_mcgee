
with polymer as (
    select a.account_id,a.name,a.type,
    a.class,a.code,'POLYMER'as source from

{{source("polymer","account")}} as a
left join {{source('polymer','journal_line')}} as jl
on a.account_id=jl.account_id
where jl.account_id is null

)
,
reactivate as
(

    select a.account_id,a.name,a.type,
    a.class,a.code,'REACTIVATE' as source from
    {{source('reactivate','account')}} as a
    left join {{source('reactivate','journal_line')}} as jl
    on a.account_id=jl.account_id
    where jl.account_id is null
)

select * from polymer

union all

select * from reactivate

