## Start Job
   ```
   cd ~/git/scray/scray-hdfs-writer/modules/compaction-job
   ./bin/submit-job.sh  --local-mode --master spark://127.0.0.1:7077 --total-executor-cores 3 -m  spark://127.0.0.1:7077 -b
   ```