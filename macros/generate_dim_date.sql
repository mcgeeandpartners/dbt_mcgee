{% macro generate_date_dim() -%}

select
    {{ dbt_utils.generate_surrogate_key(["DATE_COLUMN"]) }} as date_key,
    date_column as date,
    full_date_desc,
    day_num_in_week,
    day_num_in_month,
    day_num_in_year,
    day_name,
    day_abbrev,
    weekday_ind,
    us_holiday_ind,
    company_holiday_ind,
    month_end_ind,
    week_begin_date_nkey,
    week_begin_date,
    week_end_date_nkey,
    week_end_date,
    week_num_in_year,
    month_name,
    month_abbrev,
    month_num_in_year,
    yearmonth,
    current_quarter as quarter,
    yearquarter,
    current_year as year,
    fiscal_week_num,
    fiscal_month_num,
    fiscal_quarter,
    fiscal_halfyear,
    fiscal_year,
    sql_timestamp,
    current_row_ind,
    effective_date,
    expira_date as expiration_date
from
    (  /* <<Modify date for preferred table start date*/
        select
            to_date('2010-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') as dd,
            seq1() as sl,
            row_number() over (order by sl) as row_numbers,
            dateadd(day, row_numbers, dd) as v_date,
            case
                when date_part(dd, v_date) < 10 and date_part(mm, v_date) > 9
                then
                    date_part(year, v_date)
                    || date_part(mm, v_date)
                    || '0'
                    || date_part(dd, v_date)
                when date_part(dd, v_date) < 10 and date_part(mm, v_date) < 10
                then
                    date_part(year, v_date)
                    || '0'
                    || date_part(mm, v_date)
                    || '0'
                    || date_part(dd, v_date)
                when date_part(dd, v_date) > 9 and date_part(mm, v_date) < 10
                then
                    date_part(year, v_date)
                    || '0'
                    || date_part(mm, v_date)
                    || date_part(dd, v_date)
                when date_part(dd, v_date) > 9 and date_part(mm, v_date) > 9
                then
                    date_part(year, v_date)
                    || date_part(mm, v_date)
                    || date_part(dd, v_date)
            end as date_pkey,
            v_date as date_column,
            dayname(dateadd(day, row_numbers, dd)) as day_name_1,
            case
                when dayname(dateadd(day, row_numbers, dd)) = 'Mon'
                then 'Monday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Tue'
                then 'Tuesday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Wed'
                then 'Wednesday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Thu'
                then 'Thursday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Fri'
                then 'Friday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Sat'
                then 'Saturday'
                when dayname(dateadd(day, row_numbers, dd)) = 'Sun'
                then 'Sunday'
            end
            || ', '
            || case
                when monthname(dateadd(day, row_numbers, dd)) = 'Jan'
                then 'January'
                when monthname(dateadd(day, row_numbers, dd)) = 'Feb'
                then 'February'
                when monthname(dateadd(day, row_numbers, dd)) = 'Mar'
                then 'March'
                when monthname(dateadd(day, row_numbers, dd)) = 'Apr'
                then 'April'
                when monthname(dateadd(day, row_numbers, dd)) = 'May'
                then 'May'
                when monthname(dateadd(day, row_numbers, dd)) = 'Jun'
                then 'June'
                when monthname(dateadd(day, row_numbers, dd)) = 'Jul'
                then 'July'
                when monthname(dateadd(day, row_numbers, dd)) = 'Aug'
                then 'August'
                when monthname(dateadd(day, row_numbers, dd)) = 'Sep'
                then 'September'
                when monthname(dateadd(day, row_numbers, dd)) = 'Oct'
                then 'October'
                when monthname(dateadd(day, row_numbers, dd)) = 'Nov'
                then 'November'
                when monthname(dateadd(day, row_numbers, dd)) = 'Dec'
                then 'December'
            end
            || ' '
            || to_varchar(dateadd(day, row_numbers, dd), ' dd, yyyy') as full_date_desc,
            dateadd(day, row_numbers, dd) as v_date_1,
            dayofweek(v_date_1) + 1 as day_num_in_week,
            date_part(dd, v_date_1) as day_num_in_month,
            dayofyear(v_date_1) as day_num_in_year,
            case
                when dayname(v_date_1) = 'Mon'
                then 'Monday'
                when dayname(v_date_1) = 'Tue'
                then 'Tuesday'
                when dayname(v_date_1) = 'Wed'
                then 'Wednesday'
                when dayname(v_date_1) = 'Thu'
                then 'Thursday'
                when dayname(v_date_1) = 'Fri'
                then 'Friday'
                when dayname(v_date_1) = 'Sat'
                then 'Saturday'
                when dayname(v_date_1) = 'Sun'
                then 'Sunday'
            end as day_name,
            dayname(dateadd(day, row_numbers, dd)) as day_abbrev,
            case
                when dayname(v_date_1) = 'Sun' and dayname(v_date_1) = 'Sat'
                then 'Not-Weekday'
                else 'Weekday'
            end as weekday_ind,
            case
                when
                    (
                        date_pkey = date_part(year, v_date) || '0101'
                        or date_pkey = date_part(year, v_date) || '0704'
                        or date_pkey = date_part(year, v_date) || '1225'
                        or date_pkey = date_part(year, v_date) || '1226'
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Wed'
                    and dateadd(day, -2, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Thu'
                    and dateadd(day, -3, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Fri'
                    and dateadd(day, -4, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Sat'
                    and dateadd(day, -5, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Sun'
                    and dateadd(day, -6, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Mon'
                    and last_day(v_date_1) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'May'
                    and dayname(last_day(v_date_1)) = 'Tue'
                    and dateadd(day, -1, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Wed'
                    and dateadd(day, 5, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Thu'
                    and dateadd(day, 4, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Fri'
                    and dateadd(day, 3, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Sat'
                    and dateadd(day, 2, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Sun'
                    and dateadd(day, 1, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Mon'
                    and date_part(year, v_date_1) || '-09-01' = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Tue'
                    and dateadd(day, 6, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Wed'
                    and (
                        dateadd(day, 23, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 22, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Thu'
                    and (
                        dateadd(day, 22, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 21, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Fri'
                    and (
                        dateadd(day, 21, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 20, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Sat'
                    and (
                        dateadd(day, 27, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 26, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Sun'
                    and (
                        dateadd(day, 26, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 25, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Mon'
                    and (
                        dateadd(day, 25, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 24, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Tue'
                    and (
                        dateadd(day, 24, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                        or dateadd(day, 23, (date_part(year, v_date_1) || '-11-01'))
                        = v_date_1
                    )
                then 'Holiday'
                else 'Not-Holiday'
            end as us_holiday_ind,
            /* Modify the following for Company Specific Holidays*/
            case
                when
                    (
                        date_pkey = date_part(year, v_date) || '0101'
                        or date_pkey = date_part(year, v_date) || '0219'
                        or date_pkey = date_part(year, v_date) || '0528'
                        or date_pkey = date_part(year, v_date) || '0704'
                        or date_pkey = date_part(year, v_date) || '1225'
                    )
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Mar'
                    and dayname(last_day(v_date_1)) = 'Fri'
                    and last_day(v_date_1) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Mar'
                    and dayname(last_day(v_date_1)) = 'Sat'
                    and dateadd(day, -1, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Mar'
                    and dayname(last_day(v_date_1)) = 'Sun'
                    and dateadd(day, -2, last_day(v_date_1)) = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Tue'
                    and dateadd(day, 3, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Wed'
                    and dateadd(day, 2, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Thu'
                    and dateadd(day, 1, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Fri'
                    and date_part(year, v_date_1) || '-04-01' = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Wed'
                    and dateadd(day, 5, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Thu'
                    and dateadd(day, 4, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Fri'
                    and dateadd(day, 3, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Sat'
                    and dateadd(day, 2, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Sun'
                    and dateadd(day, 1, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Mon'
                    and date_part(year, v_date_1) || '-04-01' = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Apr'
                    and dayname(date_part(year, v_date_1) || '-04-01') = 'Tue'
                    and dateadd(day, 6, (date_part(year, v_date_1) || '-04-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Wed'
                    and dateadd(day, 5, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Thu'
                    and dateadd(day, 4, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Fri'
                    and dateadd(day, 3, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Sat'
                    and dateadd(day, 2, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Sun'
                    and dateadd(day, 1, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Mon'
                    and date_part(year, v_date_1) || '-09-01' = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Sep'
                    and dayname(date_part(year, v_date_1) || '-09-01') = 'Tue'
                    and dateadd(day, 6, (date_part(year, v_date_1) || '-09-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Wed'
                    and dateadd(day, 23, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Thu'
                    and dateadd(day, 22, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Fri'
                    and dateadd(day, 21, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Sat'
                    and dateadd(day, 27, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Sun'
                    and dateadd(day, 26, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Mon'
                    and dateadd(day, 25, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                when
                    monthname(v_date_1) = 'Nov'
                    and dayname(date_part(year, v_date_1) || '-11-01') = 'Tue'
                    and dateadd(day, 24, (date_part(year, v_date_1) || '-11-01'))
                    = v_date_1
                then 'Holiday'
                else 'Not-Holiday'
            end as company_holiday_ind,
            case
                when last_day(v_date_1) = v_date_1 then 'Month-end' else 'Not-Month-end'
            end as month_end_ind,

            case
                when
                    date_part(mm, date_trunc('week', v_date_1)) < 10
                    and date_part(dd, date_trunc('week', v_date_1)) < 10
                then
                    date_part(yyyy, date_trunc('week', v_date_1))
                    || '0'
                    || date_part(mm, date_trunc('week', v_date_1))
                    || '0'
                    || date_part(dd, date_trunc('week', v_date_1))
                when
                    date_part(mm, date_trunc('week', v_date_1)) < 10
                    and date_part(dd, date_trunc('week', v_date_1)) > 9
                then
                    date_part(yyyy, date_trunc('week', v_date_1))
                    || '0'
                    || date_part(mm, date_trunc('week', v_date_1))
                    || date_part(dd, date_trunc('week', v_date_1))
                when
                    date_part(mm, date_trunc('week', v_date_1)) > 9
                    and date_part(dd, date_trunc('week', v_date_1)) < 10
                then
                    date_part(yyyy, date_trunc('week', v_date_1))
                    || date_part(mm, date_trunc('week', v_date_1))
                    || '0'
                    || date_part(dd, date_trunc('week', v_date_1))
                when
                    date_part(mm, date_trunc('week', v_date_1)) > 9
                    and date_part(dd, date_trunc('week', v_date_1)) > 9
                then
                    date_part(yyyy, date_trunc('week', v_date_1))
                    || date_part(mm, date_trunc('week', v_date_1))
                    || date_part(dd, date_trunc('week', v_date_1))
            end as week_begin_date_nkey,
            date_trunc('week', v_date_1) as week_begin_date,

            case
                when
                    date_part(mm, last_day(v_date_1, 'week')) < 10
                    and date_part(dd, last_day(v_date_1, 'week')) < 10
                then
                    date_part(yyyy, last_day(v_date_1, 'week'))
                    || '0'
                    || date_part(mm, last_day(v_date_1, 'week'))
                    || '0'
                    || date_part(dd, last_day(v_date_1, 'week'))
                when
                    date_part(mm, last_day(v_date_1, 'week')) < 10
                    and date_part(dd, last_day(v_date_1, 'week')) > 9
                then
                    date_part(yyyy, last_day(v_date_1, 'week'))
                    || '0'
                    || date_part(mm, last_day(v_date_1, 'week'))
                    || date_part(dd, last_day(v_date_1, 'week'))
                when
                    date_part(mm, last_day(v_date_1, 'week')) > 9
                    and date_part(dd, last_day(v_date_1, 'week')) < 10
                then
                    date_part(yyyy, last_day(v_date_1, 'week'))
                    || date_part(mm, last_day(v_date_1, 'week'))
                    || '0'
                    || date_part(dd, last_day(v_date_1, 'week'))
                when
                    date_part(mm, last_day(v_date_1, 'week')) > 9
                    and date_part(dd, last_day(v_date_1, 'week')) > 9
                then
                    date_part(yyyy, last_day(v_date_1, 'week'))
                    || date_part(mm, last_day(v_date_1, 'week'))
                    || date_part(dd, last_day(v_date_1, 'week'))
            end as week_end_date_nkey,
            last_day(v_date_1, 'week') as week_end_date,
            week(v_date_1) as week_num_in_year,
            case
                when monthname(v_date_1) = 'Jan'
                then 'January'
                when monthname(v_date_1) = 'Feb'
                then 'February'
                when monthname(v_date_1) = 'Mar'
                then 'March'
                when monthname(v_date_1) = 'Apr'
                then 'April'
                when monthname(v_date_1) = 'May'
                then 'May'
                when monthname(v_date_1) = 'Jun'
                then 'June'
                when monthname(v_date_1) = 'Jul'
                then 'July'
                when monthname(v_date_1) = 'Aug'
                then 'August'
                when monthname(v_date_1) = 'Sep'
                then 'September'
                when monthname(v_date_1) = 'Oct'
                then 'October'
                when monthname(v_date_1) = 'Nov'
                then 'November'
                when monthname(v_date_1) = 'Dec'
                then 'December'
            end as month_name,
            monthname(v_date_1) as month_abbrev,
            month(v_date_1) as month_num_in_year,
            case
                when month(v_date_1) < 10
                then year(v_date_1) || '-0' || month(v_date_1)
                else year(v_date_1) || '-' || month(v_date_1)
            end as yearmonth,
            quarter(v_date_1) as current_quarter,
            year(v_date_1) || '-0' || quarter(v_date_1) as yearquarter,
            year(v_date_1) as current_year,
            /* Modify the following based on company fiscal year - assumes Jan 01*/
            to_date(year(v_date_1) || '-07-01', 'YYYY-MM-DD') as fiscal_cur_year,
            to_date(year(v_date_1) -1 || '-07-01', 'YYYY-MM-DD') as fiscal_prev_year,
            case
                when v_date_1 < fiscal_cur_year
                then datediff('week', fiscal_prev_year, v_date_1)
                else datediff('week', fiscal_cur_year, v_date_1)
            end as fiscal_week_num,
            decode(
                datediff('MONTH', fiscal_cur_year, v_date_1) + 1,
                -5,
                7,
                -4,
                8,
                -3,
                9,
                datediff('MONTH', fiscal_cur_year, v_date_1) + 1
            ) as fiscal_month_num,
            case
                when quarter(v_date_1) = 4
                then 2
                when quarter(v_date_1) = 3
                then 1
                when quarter(v_date_1) = 2
                then 4
                when quarter(v_date_1) = 1
                then 3
            end as fiscal_quarter,

            case
                when quarter(v_date_1) = 4
                then 1
                when quarter(v_date_1) = 3
                then 1
                when quarter(v_date_1) = 1
                then 2
                when quarter(v_date_1) = 2
                then 2
            end as fiscal_halfyear,
            'FY' || case
                when month(v_date_1) < 7
                then year(fiscal_cur_year)
                else year(fiscal_cur_year) + 1
            end as fiscal_year,
            to_timestamp_ntz(v_date) as sql_timestamp,
            'Y' as current_row_ind,
            to_date(current_timestamp) as effective_date,
            to_date('9999-12-31') as expira_date
        from table(generator(rowcount => 8401))
    ) v

{%- endmacro %}
