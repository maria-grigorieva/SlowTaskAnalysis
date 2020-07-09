SELECT s.pandaid,
       s.modiftime_extended,
       trunc(s.modiftime_extended, 'DDD') as date_truncated,
       s.jobstatus,
       s.computingsite,
       s.modificationhost,
       j.attemptnr,
       j.jobstatus as final_status,
       j.exeerrorcode, j.exeerrordiag,
       j.superrorcode, j.superrordiag,
       j.ddmerrorcode, j.ddmerrordiag,
       j.taskbuffererrorcode, j.taskbuffererrordiag,
       j.piloterrorcode, j.piloterrordiag,
      (CASE WHEN INSTR(LOWER(specialhandling), 'sj') > 0
        THEN 'SCOUT'
        ELSE 'NOT_SCOUT' END) as is_scout
FROM ATLAS_PANDA.JOBS_STATUSLOG s
INNER JOIN ATLAS_PANDAARCH.JOBSARCHIVED j ON(s.pandaid = j.pandaid)
WHERE jeditaskid = {}