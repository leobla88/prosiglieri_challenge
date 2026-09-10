-- Hour-of-day dimension powering the "optimal promotion time" business
-- question. BigQuery equivalent: UNNEST(GENERATE_ARRAY(0, 23)) AS hour_of_day.
select
    h                                                     as hour_of_day,
    case
        when h between 0 and 5  then 'Night (00-05)'
        when h between 6 and 11 then 'Morning (06-11)'
        when h between 12 and 17 then 'Afternoon (12-17)'
        else 'Evening (18-23)'
    end                                                   as day_part,
    case
        when h between 0 and 5  then 1
        when h between 6 and 11 then 2
        when h between 12 and 17 then 3
        else 4
    end                                                   as day_part_sort
from generate_series(0, 23) as t(h)
