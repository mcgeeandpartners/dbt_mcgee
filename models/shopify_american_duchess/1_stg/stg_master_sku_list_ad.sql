with product_details AS (
    select
        lower(p.title) as product_title
        , lower(listagg(pt.value, ', ')) as product_tags
        , lower(listagg(c.title, ', ')) as collection_titles
    from {{ source('shopify_ad', 'product') }} as p
    left join {{ source('shopify_ad', 'product_tag') }} as pt on p.id = pt.product_id
    left join {{ source('shopify_ad', 'collection_product') }} as cp on p.id = cp.product_id
    left join {{ source('shopify_ad', 'collection') }} as c on c.id = cp.collection_id
    group by 1
)
, products_oli AS (
    select distinct 
          product_title
        , null as product_tags
        , null as collection_titles
    from {{ ref('transform_order_line_item_ad') }}
)
, product_details_full as(
    select * from product_details
    union all
    select * from products_oli
)
, product_mapping_temp AS (
    SELECT 
        pd.product_title
        ,case 
            when position('imperfect ', pd.product_title) > 0 
                then substr(pd.product_title, position('imperfect ', pd.product_title) + length('imperfect '))
            when position('deadstock ', pd.product_title) > 0 
                then substr(pd.product_title, position('deadstock ', pd.product_title) + length('deadstock '))
            when position('pre-order ', pd.product_title) > 0 
                then substr(pd.product_title, position('pre-order ', pd.product_title) + length('pre-order '))
            when position('imperfect/deadstock ', pd.product_title) > 0 
                then substr(pd.product_title, position('imperfect/deadstock ', pd.product_title) + length('imperfect/deadstock '))
            else pd.product_title
        end as product_title_temp
        , pd.product_tags
        , CASE
            WHEN position('ladies', pd.product_tags) > 0 
                OR position('ladies' in pd.product_title) >0 
                OR position('womens', pd.product_tags) >0 
                OR contains(pd.product_title, 'women\'s')
                /*Some styles I have to manually map*/ 
                OR contains(pd.product_title, 'addie')
                OR contains(pd.product_title, 'alice') 
                OR contains(pd.product_title, 'alpine')
                OR contains(pd.product_title, 'amanda') 
                OR contains(pd.product_title, 'amelie')
                OR contains(pd.product_title, 'anna may') 
                OR contains(pd.product_title, 'antoinette') 
                OR contains(pd.product_title, 'alpen') 
                OR contains(pd.product_title, 'aspen') 
                OR contains(pd.product_title, 'astoria') 
                OR contains(pd.product_title, 'ava')
                OR contains(pd.product_title, 'balmoral') 
                OR contains(pd.product_title, 'bellatrix') 
                OR contains(pd.product_title, 'belleclaire') 
                OR contains(pd.product_title, 'bellevue') 
                OR contains(pd.product_title, 'bernadette')
                OR contains(pd.product_title, 'bernhardt') 
                OR contains(pd.product_title, 'bertie') 
                OR contains(pd.product_title, 'bessie') 
                OR contains(pd.product_title, 'betty')
                OR contains(pd.product_title, 'bronte')
                OR contains(pd.product_title, 'bristol')
                OR contains(pd.product_title, 'camille')
                OR contains(pd.product_title, 'cambridge')
                OR contains(pd.product_title, 'carlotta')
                OR contains(pd.product_title, 'cicero')
                OR contains(pd.product_title, 'claire') 
                OR contains(pd.product_title, 'claremont')
                OR contains(pd.product_title, 'cora')
                OR contains(pd.product_title, 'colette')
                OR contains(pd.product_title, 'cosette')
                OR contains(pd.product_title, 'daphne') 
                OR contains(pd.product_title, 'diana')
                OR contains(pd.product_title, 'dashwood')
                OR contains(pd.product_title, 'devonshire')
                OR contains(pd.product_title, 'dolores')
                OR contains(pd.product_title, 'dunmore') 
                OR contains(pd.product_title, 'eliza') 
                OR contains(pd.product_title, 'emma') 
                OR contains(pd.product_title, 'emily')
                OR contains(pd.product_title, 'evelyn')
                OR contains(pd.product_title, 'eve') 
                OR contains(pd.product_title, 'follies') 
                OR contains(pd.product_title, 'fraser') 
                OR contains(pd.product_title, 'garrick')
                OR contains(pd.product_title, 'gabrielle')
                OR contains(pd.product_title, 'galaxy')
                OR contains(pd.product_title, 'gamma')
                OR contains(pd.product_title, 'gatsby') 
                OR contains(pd.product_title, 'gettysburg')
                OR contains(pd.product_title, 'georgiana')
                OR contains(pd.product_title, 'gibson')
                OR contains(pd.product_title, 'ghillie')
                OR contains(pd.product_title, 'ginger')
                OR contains(pd.product_title, 'gladys')
                OR contains(pd.product_title, 'greta') 
                OR contains(pd.product_title, 'halina') 
                OR contains(pd.product_title, 'harriet')
                OR contains(pd.product_title, 'hartfield')
                OR contains(pd.product_title, 'hazel')
                OR contains(pd.product_title, 'highbury') 
                OR contains(pd.product_title, 'hepburn') 
                OR contains(pd.product_title, 'ingrid')
                OR contains(pd.product_title, 'jane')
                OR contains(pd.product_title, 'janie')
                OR contains(pd.product_title, 'jojo')
                OR contains(pd.product_title, 'karolina')
                OR contains(pd.product_title, 'kedwardian')
                OR contains(pd.product_title, 'kensington')
                OR contains(pd.product_title, 'kaci')
                OR contains(pd.product_title, 'keckley')
                OR contains(pd.product_title, 'kiki')
                OR contains(pd.product_title, 'lana')
                OR contains(pd.product_title, 'lacey')
                OR contains(pd.product_title, 'larkspur')
                OR contains(pd.product_title, 'lester')
                OR contains(pd.product_title, 'linden') 
                OR contains(pd.product_title, 'lila')
                OR contains(pd.product_title, 'londoner')
                OR contains(pd.product_title, 'lola')
                OR contains(pd.product_title, 'lulu') 
                OR contains(pd.product_title, 'lupita')
                OR contains(pd.product_title, 'lucille')
                OR contains(pd.product_title, 'mae') 
                OR contains(pd.product_title, 'manhattan') 
                OR contains(pd.product_title, 'mansfield')
                OR contains(pd.product_title, 'mandy')
                OR contains(pd.product_title, 'marilyn')
                OR contains(pd.product_title, 'maria') 
                OR contains(pd.product_title, 'matis')
                OR contains(pd.product_title, 'matrix')
                OR contains(pd.product_title, 'moliere')
                OR contains(pd.product_title, 'nankeen')
                OR contains(pd.product_title, 'nina')
                OR contains(pd.product_title, 'nita')
                OR contains(pd.product_title, 'noire')
                OR contains(pd.product_title, 'panther')
                OR contains(pd.product_title, 'paris')
                OR contains(pd.product_title, 'peggy') 
                OR contains(pd.product_title, 'pemberley')
                OR contains(pd.product_title, 'pompadour')
                OR contains(pd.product_title, 'poppy')
                OR contains(pd.product_title, 'renoir')
                OR contains(pd.product_title, 'rosie')
                OR contains(pd.product_title, 'rita')
                OR contains(pd.product_title, 'roxy')
                OR contains(pd.product_title, 'rush')
                OR contains(pd.product_title, 'savoy')
                OR contains(pd.product_title, 'seabury')
                OR contains(pd.product_title, 'sergi')
                OR contains(pd.product_title, 'schuyler') 
                OR contains(pd.product_title, 'shayna')
                OR contains(pd.product_title, 'shana')
                OR contains(pd.product_title, 'shirley')
                OR contains(pd.product_title, 'shoreline')
                OR contains(pd.product_title, 'spectator')
                OR contains(pd.product_title, 'sophie') 
                OR contains(pd.product_title, 'souffle')
                OR contains(pd.product_title, 'sylvia')
                OR contains(pd.product_title, 'tango') 
                OR contains(pd.product_title, 'tavistock') 
                OR contains(pd.product_title, 'theda') 
                OR contains(pd.product_title, 'tissot') 
                OR contains(pd.product_title, 'tia')
                OR contains(pd.product_title, 'tyra')
                OR contains(pd.product_title, 'vienna')
                OR contains(pd.product_title, 'victoria')
                OR contains(pd.product_title, 'vista')
                OR contains(pd.product_title, 'viola')
                OR contains(pd.product_title, 'zella')
                
            THEN 'Ladies'
                WHEN position('mens ', pd.product_tags) >0 
                OR position('men ' in pd.product_title) > 0 
                OR contains(pd.product_title, 'albert') 
                OR contains(pd.product_title, 'dandy')
                OR contains(pd.product_title, 'frederick')
                OR contains(pd.product_title, 'hamilton')
                OR contains(pd.product_title, 'hessian')
                OR contains(pd.product_title, 'lawrence')
                OR contains(pd.product_title, 'louis')
                OR contains(pd.product_title, 'marlowe')
                OR contains(pd.product_title, 'noble')
                OR contains(pd.product_title, 'stratford')
            THEN 'Mens'
                WHEN position('book', pd.product_tags) >0 
                OR position('pattern' in pd.product_title) >0 
            THEN 'Books & Patterns'
                WHEN position('stockings', pd.product_tags) >0 
                OR contains(pd.product_title, 'stockings') 
                OR position('buckle'in pd.product_title) >0
                OR position('clip'in pd.product_title) >0
                OR position('pin ' in pd.product_title) >0
                OR position('button' in pd.product_title) >0
                OR position('insoles' in pd.product_title) >0 
                OR position('cushions' in pd.product_title) >0
                OR position('pads' in pd.product_title) >0
                OR position('laces' in pd.product_title) >0
                OR position('angelus' in pd.product_title) >0 
                OR position('stretcher' in pd.product_title) >0
                OR position('sponge' in pd.product_title) >0 
                OR position('cleaner' in pd.product_title) >0 
                OR position('dye' in pd.product_title) >0 
                
            THEN 'Accessories'
                WHEN position('gift certificate' in pd.product_title) >0 
            THEN 'Gift Certificate'
            ELSE 'Misc'
        END AS product_category
        
        , CASE
            WHEN position('footwear', pd.product_tags) >0 
                OR position('button-hook', pd.product_tags) >0
                /*Womens*/
                OR contains(pd.product_title, 'addie')
                OR contains(pd.product_title, 'alice') 
                OR contains(pd.product_title, 'alpine')
                OR contains(pd.product_title, 'amanda') 
                OR contains(pd.product_title, 'amelie')
                OR contains(pd.product_title, 'anna may') 
                OR contains(pd.product_title, 'antoinette') 
                OR contains(pd.product_title, 'alpen') 
                OR contains(pd.product_title, 'aspen') 
                OR contains(pd.product_title, 'astoria') 
                OR contains(pd.product_title, 'ava')
                OR contains(pd.product_title, 'balmoral') 
                OR contains(pd.product_title, 'bellatrix') 
                OR contains(pd.product_title, 'belleclaire') 
                OR contains(pd.product_title, 'bellevue') 
                OR contains(pd.product_title, 'bernadette')
                OR contains(pd.product_title, 'bernhardt') 
                OR contains(pd.product_title, 'bertie') 
                OR contains(pd.product_title, 'bessie') 
                OR contains(pd.product_title, 'betty')
                OR contains(pd.product_title, 'bronte')
                OR contains(pd.product_title, 'bristol')
                OR contains(pd.product_title, 'camille')
                OR contains(pd.product_title, 'cambridge')
                OR contains(pd.product_title, 'carlotta')
                OR contains(pd.product_title, 'cicero')
                OR contains(pd.product_title, 'claire') 
                OR contains(pd.product_title, 'claremont')
                OR contains(pd.product_title, 'cora')
                OR contains(pd.product_title, 'colette')
                OR contains(pd.product_title, 'cosette')
                OR contains(pd.product_title, 'daphne') 
                OR contains(pd.product_title, 'diana')
                OR contains(pd.product_title, 'dashwood')
                OR contains(pd.product_title, 'devonshire')
                OR contains(pd.product_title, 'dolores')
                OR contains(pd.product_title, 'dunmore') 
                OR contains(pd.product_title, 'eliza') 
                OR contains(pd.product_title, 'emma') 
                OR contains(pd.product_title, 'emily')
                OR contains(pd.product_title, 'evelyn')
                OR contains(pd.product_title, 'eve') 
                OR contains(pd.product_title, 'follies') 
                OR contains(pd.product_title, 'fraser') 
                OR contains(pd.product_title, 'garrick')
                OR contains(pd.product_title, 'gabrielle')
                OR contains(pd.product_title, 'galaxy')
                OR contains(pd.product_title, 'gamma')
                OR contains(pd.product_title, 'gatsby') 
                OR contains(pd.product_title, 'gettysburg')
                OR contains(pd.product_title, 'georgiana')
                OR contains(pd.product_title, 'gibson')
                OR contains(pd.product_title, 'ghillie')
                OR contains(pd.product_title, 'ginger')
                OR contains(pd.product_title, 'gladys')
                OR contains(pd.product_title, 'greta') 
                OR contains(pd.product_title, 'halina') 
                OR contains(pd.product_title, 'harriet')
                OR contains(pd.product_title, 'hartfield')
                OR contains(pd.product_title, 'hazel')
                OR contains(pd.product_title, 'highbury') 
                OR contains(pd.product_title, 'hepburn') 
                OR contains(pd.product_title, 'ingrid')
                OR contains(pd.product_title, 'jane')
                OR contains(pd.product_title, 'janie')
                OR contains(pd.product_title, 'jojo')
                OR contains(pd.product_title, 'karolina')
                OR contains(pd.product_title, 'kedwardian')
                OR contains(pd.product_title, 'kensington')
                OR contains(pd.product_title, 'kaci')
                OR contains(pd.product_title, 'keckley')
                OR contains(pd.product_title, 'kiki')
                OR contains(pd.product_title, 'lana')
                OR contains(pd.product_title, 'lacey')
                OR contains(pd.product_title, 'larkspur')
                OR contains(pd.product_title, 'lester')
                OR contains(pd.product_title, 'lido')
                OR contains(pd.product_title, 'linden') 
                OR contains(pd.product_title, 'lila')
                OR contains(pd.product_title, 'lilith')
                OR contains(pd.product_title, 'londoner')
                OR contains(pd.product_title, 'lola')
                OR contains(pd.product_title, 'lulu') 
                OR contains(pd.product_title, 'lupita')
                OR contains(pd.product_title, 'lucille')
                OR contains(pd.product_title, 'mae') 
                OR contains(pd.product_title, 'manhattan') 
                OR contains(pd.product_title, 'mansfield')
                OR contains(pd.product_title, 'mandy')
                OR contains(pd.product_title, 'marilyn')
                OR contains(pd.product_title, 'maria')
                OR contains(pd.product_title, 'mary')
                OR contains(pd.product_title, 'matis')
                OR contains(pd.product_title, 'matrix')
                OR contains(pd.product_title, 'meme')
                OR contains(pd.product_title, 'moliere')
                OR contains(pd.product_title, 'nankeen')
                OR contains(pd.product_title, 'nell')
                OR contains(pd.product_title, 'nina')
                OR contains(pd.product_title, 'nita')
                OR contains(pd.product_title, 'noire')
                OR contains(pd.product_title, 'panther')
                OR contains(pd.product_title, 'paris')
                OR contains(pd.product_title, 'parker')
                OR contains(pd.product_title, 'peggy') 
                OR contains(pd.product_title, 'pemberley')
                OR contains(pd.product_title, 'penelope')
                OR contains(pd.product_title, 'pompadour')
                OR contains(pd.product_title, 'poppy')
                OR contains(pd.product_title, 'rainey')
                OR contains(pd.product_title, 'renoir')
                OR contains(pd.product_title, 'rosie')
                OR contains(pd.product_title, 'rita')
                OR contains(pd.product_title, 'roxy')
                OR contains(pd.product_title, 'rush')
                OR contains(pd.product_title, 'savoy')
                OR contains(pd.product_title, 'seabury')
                OR contains(pd.product_title, 'sergi')
                OR contains(pd.product_title, 'schuyler') 
                OR contains(pd.product_title, 'shayna')
                OR contains(pd.product_title, 'shana')
                OR contains(pd.product_title, 'shirley')
                OR contains(pd.product_title, 'shoreline')
                OR contains(pd.product_title, 'spectator')
                OR contains(pd.product_title, 'sophie') 
                OR contains(pd.product_title, 'souffle')
                OR contains(pd.product_title, 'sylvia')
                OR contains(pd.product_title, 'tango') 
                OR contains(pd.product_title, 'tavistock') 
                OR contains(pd.product_title, 'theda') 
                OR contains(pd.product_title, 'tissot') 
                OR contains(pd.product_title, 'tia')
                OR contains(pd.product_title, 'tyra')
                OR contains(pd.product_title, 'vienna')
                OR contains(pd.product_title, 'victoria')
                OR contains(pd.product_title, 'vista')
                OR contains(pd.product_title, 'viola')
                OR contains(pd.product_title, 'zella')
                /*Men's*/
                OR contains(pd.product_title, 'albert') 
                OR contains(pd.product_title, 'dandy')
                OR contains(pd.product_title, 'frederick')
                OR contains(pd.product_title, 'hamilton')
                OR contains(pd.product_title, 'hessian')
                OR contains(pd.product_title, 'lawrence')
                OR contains(pd.product_title, 'louis')
                OR contains(pd.product_title, 'marlowe')
                OR contains(pd.product_title, 'noble')
                OR contains(pd.product_title, 'stratford')
            THEN 'Footwear'
            WHEN position('pattern' in pd.product_title) >0  THEN 'Sewing Patterns'
            WHEN position('book', pd.product_tags) >0 THEN 'Books'
            WHEN position('gift certificate' in pd.product_title) >0 THEN 'Gift Certificate'
            WHEN position('stockings', pd.product_tags) > 0 or contains(pd.product_title, 'stockings') THEN 'Stockings'
            WHEN position('pin ', pd.product_title) >0 
                OR position('clip ', pd.product_title) >0
                OR position('buckle'in pd.product_title) >0 
                OR position('button' in pd.product_title) >0 
            THEN 'Shoe Buckles & Clips'
            WHEN position('insoles' in pd.product_title) >0 
                OR position('cushions' in pd.product_title) >0
                OR position('pads' in pd.product_title) >0
            THEN 'Shoe Inserts'
            WHEN position('laces' in pd.product_title) >0 THEN 'Shoe Laces'
            WHEN position('angelus' in pd.product_title) >0 
                OR position('stretcher' in pd.product_title) >0
                OR position('sponge' in pd.product_title) >0 
                OR position('cleaner' in pd.product_title) >0 
                OR position('dye' in pd.product_title) >0 
            THEN 'Shoe Care'
            ELSE 'Misc'
        END AS product_type
        
        , CASE
            WHEN position('booties', pd.product_tags) >0  OR position('booties' in pd.product_title)>0 THEN 'Booties'
            WHEN position('oxfords', pd.product_tags) >0 OR position('oxfords' in pd.product_title) >0THEN 'Oxfords'
            WHEN position('pumps', pd.product_tags) >0 OR position('pumps' in pd.product_title) >0 THEN 'Pumps'
            WHEN position('boots', pd.product_tags) >0  OR position('boots' in pd.product_title)>0 THEN 'Boots'
            WHEN position('strappy', pd.product_tags) >0 OR position('gibson' in pd.product_title) >0 THEN 'Strappy'
            WHEN position('slippers', pd.product_tags) >0 THEN 'Slippers'
            WHEN position('latchet', pd.product_tags) >0 THEN 'Latchets'
            WHEN position('mules', pd.product_tags) >0 THEN 'Mules'
            WHEN position('sandals', pd.product_tags) >0 OR position('sandals' in pd.product_title) >0 THEN 'Sandals'
            WHEN position('peep', pd.product_tags) >0 THEN 'Peep'
            WHEN position('high heels' in pd.product_title) >0 THEN 'High Heels'
            
            WHEN position('buckles',  pd.product_tags) >0 THEN 'Buckles'
            //WHEN position('button-hook',  pd.product_tags) >0 THEN 'Button Hook'
            //WHEN position('button-shoe',  pd.product_tags) >0 THEN 'Shoe Button'
            WHEN position('pattern' in pd.product_title) >0 THEN 'Sewing Patterns'
            WHEN position('book',  pd.product_tags) >0 THEN 'Books'
            WHEN position('pattern' in pd.product_title) >0 THEN 'Books & Patterns'
            WHEN position('gift certificate' in pd.product_title) >0 THEN 'Gift Certificate'
            WHEN position('stockings',  pd.product_tags) >0 THEN 'Stockings'
            WHEN position('pin', pd.product_title) >0 THEN 'Shoe Buckles & Clips'
            WHEN position('clip', pd.product_title) >0 THEN 'Shoe Buckles & Clips'
            WHEN position('buckles'in pd.product_title) >0 THEN 'Shoe Buckles & Clips'
            WHEN position('button' in pd.product_title) >0 THEN 'Shoe Buckles & Clips'
            WHEN position('insoles' in pd.product_title) >0 THEN 'Shoe Inserts'
            WHEN position('cushions' in pd.product_title) >0 THEN 'Shoe Inserts'
            WHEN position('laces' in pd.product_title) >0 THEN 'Shoe Laces'
            WHEN position('repellent' in pd.product_title) >0 THEN 'Shoe Protector'
            WHEN position('protector' in pd.product_title) >0 THEN 'Shoe Protector'
            WHEN position('cream' in pd.product_title) >0 THEN 'Shoe Conditioning'
            WHEN position('stretchers' in pd.product_title) >0 THEN 'Shoe Care'
            WHEN position('conditioner' in pd.product_title) >0 THEN 'Shoe Conditioning'
            WHEN position('sponge' in pd.product_title) >0 THEN 'Shoe Cleaning'
            WHEN position('kit' in pd.product_title) >0 THEN 'Shoe Cleaning'
            WHEN position('cleaner' in pd.product_title) >0 THEN 'Shoe Cleaning'
            WHEN position('dye' in pd.product_title) >0 THEN 'Shoe Dye'
            WHEN position('stretch' in pd.product_title) >0 THEN 'Shoe Stretcher'
            WHEN position('polishing' in pd.product_title) >0 THEN 'Shoe Polish'
            WHEN position('wax' in pd.product_title) >0 THEN 'Shoe Wax'
            
        END AS product_sub_type
        , CASE
            WHEN position('mid heel', pd.product_tags) >0 THEN '2 - 3" / 5 - 7.6 cm Mid Heel'
            WHEN position('low heel', pd.product_tags) >0 THEN '1 - 2" / 2.5 - 5 cm Low Heel'
            WHEN position('flat', pd.product_tags) >0 THEN '0 - 1" / 0 - 2.5 cm Flat'
            WHEN position('high heel', pd.product_tags) >0 THEN '3"+ / 7.6 cm + High Heel'
        END AS product_heel
        , CASE
            WHEN position('edwardian', pd.product_tags) >0 THEN 'Edwardian'
            WHEN position('victorian', pd.product_tags) >0 THEN 'Victorian'
            WHEN position('renaissance', pd.product_tags) >0 THEN 'Renaissance'
            WHEN position('18th century', pd.product_tags) >0 THEN '18th Century'
            WHEN position('regency', pd.product_tags) >0 THEN 'Regency'
            WHEN position('vintage', pd.product_tags) >0 THEN 'Vintage'
        END AS product_era
        , CASE
            WHEN position('vintage', pd.product_tags) >0 THEN 'Vintage'
            WHEN position('cottage style', pd.product_tags) >0 THEN 'Cottage Style'
            WHEN position('dark academia', pd.product_tags) >0 THEN 'Dark Academia'
            WHEN position('gothic', pd.product_tags) >0 THEN 'Gothic'
            WHEN position('lolita', pd.product_tags) >0 THEN 'Lolita'
            WHEN position('steampunk', pd.product_tags) >0 THEN 'Steampunk'
            WHEN position('wedding', pd.product_tags) >0 THEN 'Wedding'
        END AS product_genre //I THINK THIS IS IN THE COLLECTIONS, ITS NOT POPULATING MUCH HERE
        , TRIM(
            CASE
            WHEN position ('(1', pd.product_title) > 0 THEN SUBSTR(pd.product_title, position ('(1', pd.product_title), position (')', pd.product_title)-position ('(1', pd.product_title))
        END,'()') AS product_decade
        , TRIM(
            CASE
                WHEN position ('(', pd.product_title) > 0 THEN LEFT(SUBSTR(pd.product_title, position ('(', pd.product_title)),POSITION(')', SUBSTR(pd.product_title, position ('(', pd.product_title))))
        END,'()') AS product_color
        , CASE
            WHEN contains(pd.product_title, 'addie') THEN 'Addie'
            WHEN contains(pd.product_title, 'albert') THEN 'Albert'
            WHEN contains(pd.product_title, 'alice') THEN 'Alice'
            WHEN contains(pd.product_title, 'alpine') THEN 'Alpine'
            WHEN contains(pd.product_title, 'amanda') THEN 'Amanda'
            WHEN contains(pd.product_title, 'amelie') THEN 'Amelie'
            WHEN contains(pd.product_title, 'anna may') THEN 'Anna May'
            WHEN contains(pd.product_title, 'antoinette') THEN 'Antoinette'
            WHEN contains(pd.product_title, 'aspen') THEN 'Aspen'
            WHEN contains(pd.product_title, 'astoria') THEN 'Astoria'
            WHEN contains(pd.product_title, 'ava') THEN 'Ava'
            WHEN contains(pd.product_title, 'balmoral') THEN 'Balmoral'
            WHEN contains(pd.product_title, 'bellatrix') THEN 'Bellatrix'
            WHEN contains(pd.product_title, 'belleclaire') THEN 'Belleclaire'
            WHEN contains(pd.product_title, 'bellevue') THEN 'Bellevue'
            WHEN contains(pd.product_title, 'bernadette') THEN 'Bernadette'
            WHEN contains(pd.product_title, 'bernhardt') THEN 'Bernhardt'
            WHEN contains(pd.product_title, 'bertie') THEN 'Bertie'
            WHEN contains(pd.product_title, 'bessie') THEN 'Bessie'
            WHEN contains(pd.product_title, 'betty') THEN 'Betty'
            WHEN contains(pd.product_title, 'camille') THEN 'Camille'
            WHEN contains(pd.product_title, 'claire') THEN 'Claire'
            WHEN contains(pd.product_title, 'colette') THEN 'Colette'
            WHEN contains(pd.product_title, 'daphne') THEN 'Daphne'
            WHEN contains(pd.product_title, 'dashwood') THEN 'Dashwood'
            WHEN contains(pd.product_title, 'devonshire') THEN 'Devonshire'
            WHEN contains(pd.product_title, 'dunmore') THEN 'Dunmore'
            WHEN contains(pd.product_title, 'eliza') THEN 'Eliza'
            WHEN contains(pd.product_title, 'emma') THEN 'Emma'
            WHEN contains(pd.product_title, 'evelyn') THEN 'Evelyn'
            WHEN contains(pd.product_title, 'follies') THEN 'Follies'
            WHEN contains(pd.product_title, 'fraser') THEN 'Fraser'
            WHEN contains(pd.product_title, 'garrick') THEN 'Garrick'
            WHEN contains(pd.product_title, 'gatsby') THEN 'Gatsby'
            WHEN contains(pd.product_title, 'georgiana') THEN 'Georgiana'
            WHEN contains(pd.product_title, 'gibson') THEN 'Gibson'
            WHEN contains(pd.product_title, 'greta') THEN 'Greta'
            WHEN contains(pd.product_title, 'hamilton') THEN 'Hamilton'
            WHEN contains(pd.product_title, 'hepburn') THEN 'Hepburn'
            WHEN contains(pd.product_title, 'hessian') THEN 'Hessian'
            WHEN contains(pd.product_title, 'kensington') THEN 'Kensington'
            WHEN contains(pd.product_title, 'linden') THEN 'Linden'
            WHEN contains(pd.product_title, 'londoner') THEN 'Londoner'
            WHEN contains(pd.product_title, 'mae') THEN 'Mae'
            WHEN contains(pd.product_title, 'manhattan') THEN 'Manhattan'
            WHEN contains(pd.product_title, 'mansfield') THEN 'Mansfield'
            WHEN contains(pd.product_title, 'maria') THEN 'Maria'
            WHEN contains(pd.product_title, 'moliere') THEN 'Moliere'
            WHEN contains(pd.product_title, 'pegggy') THEN 'Peggy'
            WHEN contains(pd.product_title, 'pompadour') THEN 'Pompadour'
            WHEN contains(pd.product_title, 'renoir') THEN 'Renoir'
            WHEN contains(pd.product_title, 'schuyler') THEN 'Schuyler'
            WHEN contains(pd.product_title, 'sophie') THEN 'Sophie'
            WHEN contains(pd.product_title, 'tango') THEN 'Tango'
            WHEN contains(pd.product_title, 'travistock') THEN 'Travistock'
            WHEN contains(pd.product_title, 'theda') THEN 'Theda'
            WHEN contains(pd.product_title, 'tissot') THEN 'Tissot'
            WHEN contains(pd.product_title, 'vienna') THEN 'Vienna'
        END AS product_style
        , CASE
            WHEN position('pre-order' in pd.product_title) >0 THEN 'Pre-Order'
            ELSE 'In-Stock'
        END AS product_is_pre_order
        ,  CASE
            WHEN POSITION('imperfect ', pd.product_title) > 0 THEN 'Imperfect'
            WHEN POSITION('deadstock ', pd.product_title) > 0 THEN 'Deadstock'
        END as product_is_clearance
    FROM product_details_full pd
)
, product_mapping as (
    SELECT DISTINCT
    product_title
    , LEFT(product_title_temp,
        CASE WHEN REGEXP_INSTR(product_title_temp , '\\(\.*')-2 >0 THEN REGEXP_INSTR(product_title_temp , '\\(\.*')-2
        ELSE length(product_title_temp)END
        ) as product_title_adj
    , product_category
    , product_type
    , product_sub_type
    , product_heel
    , product_era
    , product_genre
    , product_decade
    , product_color
    , product_style
    , product_is_pre_order
    from product_mapping_temp
)
, clean_up AS (
    SELECT 
    product_title_adj
    , max(product_category) as product_category
    , max(product_type) as product_type
    , max(product_sub_type) as product_sub_type
    , max(product_era) as product_era
    , max(product_genre) as product_genre
    , max(product_decade) as product_decade
    , max(product_style) as product_style
    , max(product_heel) as product_heel
    FROM product_mapping
    GROUP BY 1
)
, final_mapping as (
    SELECT DISTINCT
        map.product_title
        , map.product_title_adj
        , c.product_category
        , c.product_type
        , c.product_sub_type
        , c.product_heel
        , c.product_era
        , c.product_genre
        , c.product_decade
        , map.product_color
        , c.product_style
        , map.product_is_pre_order 
    FROM product_mapping map
    LEFT JOIN clean_up c ON map.product_title_adj = c.product_title_adj
)
, unique_variant_list as (
    SELECT
    product_title
    , product_variant_name
    , sum(order_line_item_price*order_line_item_units) as volume_proxy
    from aestuary_dw.shopify_american_duchess.transform_order_line_item_ad
    group by 1,2
)
, full_set AS (
    SELECT
        ul.product_variant_name
        , ul.product_title
        , ul.volume_proxy
        , map.product_title_adj
        , map.product_category
        , map.product_type
        , map.product_sub_type
        , map.product_heel
        , map.product_era
        , map.product_genre
        , map.product_decade
        , map.product_color
        , map.product_style
        , map.product_is_pre_order
    FROM unique_variant_list ul
    LEFT JOIN final_mapping map on ul.product_title = map.product_title
)
/*, filtered_list as (
    Select 
        product_title_adj
        , sum(volume_proxy) 
    from full_set
    group by 1
    having sum(volume_proxy) > 100,000
)*/
SELECT
full_set.*
FROM full_set