-- Replace <FF_DAILY_FACTORS_TABLE> with the daily Fama-French factors table
-- exposed by your institution in the ff_all schema.

select
    date,
    mktrf,
    smb,
    hml,
    umd,
    rf
from <FF_DAILY_FACTORS_TABLE>
where date between date '2010-01-01' and date '2025-12-31'
order by date;
