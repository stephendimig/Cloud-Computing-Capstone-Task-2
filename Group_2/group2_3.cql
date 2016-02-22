use mykeyspace;

select origin,dest,carrier,delayavg from results2_3 WHERE origin='LGA' AND dest='BOS' ORDER BY delayavg LIMIT 10;
select origin,dest,carrier,delayavg from results2_3 WHERE origin='BOS' AND dest='LGA' ORDER BY delayavg LIMIT 10;
select origin,dest,carrier,delayavg from results2_3 WHERE origin='OKC' AND dest='DFW' ORDER BY delayavg LIMIT 10;
select origin,dest,carrier,delayavg from results2_3 WHERE origin='MSP' AND dest='ATL' ORDER BY delayavg LIMIT 10;
