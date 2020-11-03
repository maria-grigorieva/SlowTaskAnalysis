SELECT
   jeditaskid,
   modificationtime,
   status,
   LAG(modificationtime,1)
          OVER (
            PARTITION BY jeditaskid ORDER BY modificationtime ASC) lag,
   ROUND((CAST(modificationtime as date) - LAG(CAST(modificationtime as date),1)
          OVER (
         PARTITION BY jeditaskid ORDER BY modificationtime ASC))*60*60*24, 3) delay
 FROM ATLAS_PANDA.TASKS_STATUSLOG
   where jeditaskid in ({})
ORDER BY jeditaskid, modificationtime