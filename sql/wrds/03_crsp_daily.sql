-- CRSP Daily stock file in legacy naming.
-- If your WRDS environment is already on CIZ-only naming, replace this
-- with the compatible daily security/event views your institution exposes.

select
    a.permno,
    a.date,
    a.ret,
    coalesce(b.dlret, 0) as dlret,
    a.prc,
    a.shrout
from crsp.dsf as a
left join crsp.dsedelist as b
    on a.permno = b.permno
   and a.date = b.dlstdt
where a.date between date '2010-01-01' and date '2025-12-31'
order by a.permno, a.date;
