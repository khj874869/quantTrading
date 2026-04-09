-- Preferred method:
-- 1) Use WRDS' IBES-CRSP linking table web query, or
-- 2) Build the link using ICLINK / ICLINK_CIZ and export the result.
--
-- The project expects:
-- ticker, permno, linkdt, linkenddt
--
-- If your admin exposes a materialized link table, replace <IBES_CRSP_LINK_TABLE>.

select
    ticker,
    permno,
    coalesce(sdate, date '1900-01-01') as linkdt,
    coalesce(edate, date '2100-01-01') as linkenddt
from <IBES_CRSP_LINK_TABLE>
where score in (0, 1, 2)
order by ticker, permno, linkdt;
