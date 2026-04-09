-- Compustat North America Quarterly fundamentals.
-- Common legacy table: comp.fundq
-- Some institutions expose similar content under comp_na_daily_all.* schemas.

select
    gvkey,
    datadate,
    rdq,
    sic,
    fqtr,
    fyearq,
    atq,
    ltq,
    ceqq,
    saleq,
    ibq,
    oancfy
from comp.fundq
where indfmt = 'INDL'
  and datafmt = 'STD'
  and popsrc = 'D'
  and consol = 'C'
  and datadate between date '2010-01-01' and date '2025-12-31'
order by gvkey, datadate;
