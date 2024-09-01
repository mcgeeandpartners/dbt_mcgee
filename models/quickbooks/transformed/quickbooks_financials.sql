
select je.id as journal_entry,je.transaction_date as journal_transaction_date,
jel.amount as net_amount_original,
CASE 
WHEN a.account_type in ('REVENUE','OTHERINCOME')THEN jel.amount * -1  
ELSE jel.amount * 1
END AS net_amount, 
jel.index as journal_entry_line_index,
a.id as account_id,
a.account_number,
a.account_type,
a.name as account_name,
a.classification as account_classification,
d.name as department_name,
c.name as class_name
from 
 {{ source("qb", "journal_entry") }}  je

left join  {{ source("qb", "journal_entry_line") }} jel
on je.id=jel.journal_entry_id
left join  {{ source("qb", "account") }} a
on a.id=jel.account_id
left join   {{ source("qb", "class") }} c
on c.id=jel.class_id
left join   {{ source("qb", "department") }} d 
on d.id=jel.department_id
