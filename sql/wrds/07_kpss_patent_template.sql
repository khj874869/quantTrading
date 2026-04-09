-- Replace <KPSS_PATENT_TABLE> with the patent table your institution exposes.
-- The project expects annualized firm-level counts.

select
    gvkey,
    issue_date,
    count(*) as patent_count,
    coalesce(sum(num_citations), 0) as citation_count
from <KPSS_PATENT_TABLE>
where issue_date between date '2010-01-01' and date '2025-12-31'
group by gvkey, issue_date
order by gvkey, issue_date;
