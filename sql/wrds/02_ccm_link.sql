-- CRSP/Compustat Merged link table.
-- Common link table name: crsp.ccmxpf_linktable

select
    gvkey,
    lpermno as permno,
    linkdt,
    coalesce(linkenddt, date '2100-01-01') as linkenddt,
    linktype,
    linkprim
from crsp.ccmxpf_linktable
where lpermno is not null
  and linktype in ('LC', 'LU', 'LS')
  and linkprim in ('P', 'C')
order by gvkey, permno, linkdt;
