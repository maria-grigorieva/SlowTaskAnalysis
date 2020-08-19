with first as (
    SELECT
      trunc(statechangetime, 'DDD') AS tstamp_hour,
      computingsite  AS queue,
      ROUND(sum(endtime - starttime) * 24, 3) AS walltime_hours,
      ROUND(sum(cpuconsumptiontime / (actualcorecount * 3600)), 3) as cputime_hours,
      ROUND(sum(actualcorecount), 3) as actualcorecount_total,
      ROUND(sum(corecount), 3)  AS corecount_total,
      (CASE WHEN sum(endtime - starttime) != 0
        THEN
          ROUND(((sum(cpuconsumptiontime / (actualcorecount * 3600))) / (sum(endtime - starttime) * 24)),3)
       ELSE
         0
       END) AS cpu_utilization
    FROM ATLAS_PANDAARCH.JOBSARCHIVED
    WHERE statechangetime >= to_date('{date_from}', 'yyyy-MM-dd HH24:mi:ss')
          AND statechangetime < to_date('{date_to}', 'yyyy-MM-dd HH24:mi:ss')
          AND endtime IS NOT NULL
          AND starttime IS NOT NULL
          AND eventservice is NULL
          AND produsername != 'gangarbt'
          AND computingsite in
            (SELECT siteid from atlas_pandameta.schedconfig
            where status != 'test' AND resource_type != 'gpu')
          AND jobstatus = 'finished'
    GROUP BY trunc(statechangetime, 'DDD'),
      computingsite
  UNION
          SELECT
      trunc(statechangetime, 'DDD') AS tstamp_hour,
      computingsite  AS queue,
      ROUND(sum(endtime - starttime) * 24, 3) AS walltime_hours,
      ROUND(sum(cpuconsumptiontime / (actualcorecount * 3600)), 3) as cputime_hours,
      ROUND(sum(actualcorecount), 3) as actualcorecount_total,
      ROUND(sum(corecount), 3)  AS corecount_total,
      (CASE WHEN sum(endtime - starttime) != 0
        THEN
          ROUND(((sum(cpuconsumptiontime / (actualcorecount * 3600))) / (sum(endtime - starttime) * 24)),3)
       ELSE
         0
       END) AS cpu_utilization
    FROM ATLAS_PANDA.JOBSARCHIVED4
    WHERE statechangetime >= to_date('{date_from}', 'yyyy-MM-dd HH24:mi:ss')
          AND statechangetime < to_date('{date_to}', 'yyyy-MM-dd HH24:mi:ss')
          AND endtime IS NOT NULL
          AND starttime IS NOT NULL
          AND eventservice is NULL
          AND produsername != 'gangarbt'
          AND computingsite in
            (SELECT siteid from atlas_pandameta.schedconfig
            where status != 'test' AND resource_type != 'gpu')
          AND jobstatus = 'finished'
    GROUP BY trunc(statechangetime, 'DDD'),
      computingsite
),
  status as (
SELECT trunc(statechangetime, 'DDD')                                       AS tstamp_hour,
  computingsite as queue,
  jobstatus,
  count(*) as cnt
  FROM ATLAS_PANDAARCH.JOBSARCHIVED
    WHERE statechangetime >= to_date('{date_from}', 'yyyy-MM-dd HH24:mi:ss')
          AND statechangetime < to_date('{date_to}', 'yyyy-MM-dd HH24:mi:ss')
          AND endtime IS NOT NULL
          AND starttime IS NOT NULL
    GROUP BY trunc(statechangetime, 'DDD'),
      computingsite, jobstatus
    UNION
  SELECT trunc(statechangetime, 'DDD')                                       AS tstamp_hour,
  computingsite as queue,
  jobstatus,
  count(*) as cnt
  FROM ATLAS_PANDA.JOBSARCHIVED4
    WHERE statechangetime >= to_date('{date_from}', 'yyyy-MM-dd HH24:mi:ss')
          AND statechangetime < to_date('{date_to}', 'yyyy-MM-dd HH24:mi:ss')
          AND endtime IS NOT NULL
          AND starttime IS NOT NULL
    GROUP BY trunc(statechangetime, 'DDD'),
      computingsite, jobstatus
),
  finish_fail AS (
      SELECT *
      FROM status
      PIVOT (
        sum(cnt)
        FOR jobstatus
        IN (
          'finished' finished_jobs, 'failed' failed_jobs
        )
      )
  )
  select f.tstamp_hour,
    f.queue,
    f.walltime_hours,
    f.actualcorecount_total,
    f.corecount_total,
    (CASE WHEN f.cpu_utilization > 1 THEN 1 ELSE f.cpu_utilization END) as cpu_utilization_fixed,
    f.cpu_utilization,
    f.cputime_hours,
      NVL(ff.finished_jobs,0) finished_jobs,
  NVL(ff.failed_jobs,0) failed_jobs,
      (CASE WHEN (NVL(ff.finished_jobs,0)+NVL(ff.failed_jobs,0)) != 0 THEN
     round(NVL(ff.finished_jobs,0)/(NVL(ff.finished_jobs,0)+NVL(ff.failed_jobs,0)), 3)
    ELSE 0 END) as jobs_efficiency
    FROM first f
INNER JOIN finish_fail ff ON (f.tstamp_hour = ff.tstamp_hour AND f.queue = ff.queue)
