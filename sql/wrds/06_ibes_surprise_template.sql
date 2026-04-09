-- Replace <IBES_SURPRISE_TABLE> with the surprise/actuals table available in your WRDS account.
-- The project expects:
-- ticker, statpers, fpedats, actual, surprise, surpct

select
    ticker,
    statpers,
    fpedats,
    actual,
    surprise,
    surpct
from <IBES_SURPRISE_TABLE>
where statpers between date '2010-01-01' and date '2025-12-31'
order by ticker, statpers, fpedats;
