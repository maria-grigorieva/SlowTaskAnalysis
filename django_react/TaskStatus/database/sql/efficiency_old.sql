SELECT computingsite,
       trunc(statechangetime,'DDD'),
       jobstatus,
       count(*)
FROM ATLAS_PANDAARCH.JOBSARCHIVED
WHERE statechangetime >= to_date('{}', 'yyyy-MM-dd HH24:mi:ss')
  AND statechangetime < to_date('{}', 'yyyy-MM-dd HH24:mi:ss')
  AND jobstatus IN ('finished','failed')
  AND computingsite IN ({})
GROUP BY computingsite,
         trunc(statechangetime,'DDD'),
         jobstatus