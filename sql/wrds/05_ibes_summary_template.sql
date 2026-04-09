-- Replace <IBES_SUMMARY_TABLE> with the EPS summary history table available in your WRDS account.
-- The project expects:
-- ticker, statpers, fpedats, meanest, stdev, numest, measure

select
    ticker,
    statpers,
    fpedats,
    meanest,
    stdev,
    numest,
    measure
from <IBES_SUMMARY_TABLE>
where measure = 'EPS'
  and statpers between date '2010-01-01' and date '2025-12-31'
order by ticker, statpers, fpedats;
