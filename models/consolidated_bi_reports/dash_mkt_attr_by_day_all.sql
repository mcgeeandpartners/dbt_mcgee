
SELECT 'Alice and Ames' as sub_company, * FROM {{ ref('dash_mkt_attr_by_day_alice_ames') }}
UNION
SELECT 'Little Poppy Co' as sub_company, * FROM {{ ref('dash_mkt_attr_by_day_lpc') }}