use mykeyspace;
select origin,carrier,delayavg from results2_1 WHERE origin='SRQ' ORDER BY delayavg;
select origin,carrier,delayavg from results2_1 WHERE origin='CMH' ORDER BY delayavg;
select origin,carrier,delayavg from results2_1 WHERE origin='JFK' ORDER BY delayavg;
select origin,carrier,delayavg from results2_1 WHERE origin='SEA' ORDER BY delayavg;
select origin,carrier,delayavg from results2_1 WHERE origin='BOS' ORDER BY delayavg;
