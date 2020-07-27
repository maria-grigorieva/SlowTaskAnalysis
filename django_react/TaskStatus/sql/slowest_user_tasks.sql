SELECT * FROM (
  SELECT jeditaskid,
         (TRUNC(modificationtime,'HH24') - TRUNC(starttime,'HH24')) as duration,
         status,
         prodsourcelabel
  FROM ATLAS_PANDA.JEDI_TASKS
  WHERE starttime >= to_date('{}','YYYY-MM-DD HH24:MI:SS')
    AND starttime <= to_date('{}', 'YYYY-MM-DD HH24:MI:SS')
    AND status in ('done', 'finished', 'failed', 'broken', 'aborted')
    AND prodsourcelabel = 'user'
    AND starttime IS NOT NULL
  ORDER BY duration desc
  )
WHERE rownum < 51