from rest_framework import serializers

class AllFailedJobs(serializers.Serializer):
   PANDAID = serializers.IntegerField()
   MODIFTIME_EXTENDED = serializers.DateTimeField()
   JOBSTATUS = serializers.CharField(max_length=50)
   COMPUTINGSITE = serializers.CharField(max_length=100)
   MODIFICATIONHOST = serializers.CharField(max_length=100)
   ATTEMPTNR = serializers.IntegerField()
   FINAL_STATUS = serializers.CharField(max_length=30)
   START_TS = serializers.DateTimeField()
   END_TS = serializers.DateTimeField()
   DURATION = serializers.IntegerField()
   STATUS_LEVEL = serializers.IntegerField()
   SEQUENCE = serializers.CharField(max_length=500)