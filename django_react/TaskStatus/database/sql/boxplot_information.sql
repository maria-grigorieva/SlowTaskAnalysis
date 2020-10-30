SELECT jeditaskid,
       (TRUNC(modificationtime,'HH24') - TRUNC(starttime,'HH24')) as duration,
       status
FROM ATLAS_PANDA.JEDI_TASKS
WHERE starttime >= to_date('{} 00:00:00','YYYY-MM-DD HH24:MI:SS')
  AND starttime <= to_date('{} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
  AND status in ('done', 'finished', 'failed', 'broken', 'aborted')
  AND prodsourcelabel = 'user'
  AND starttime IS NOT NULL
ORDER BY duration desc