FROM openjdk

RUN yum install procps wget -y
COPY bin    /opt/spark-job/bin
COPY conf   /opt/spark-job/conf
COPY lib    /opt/spark-job/lib
COPY target /opt/spark-job/target
  
# RUN ls -la /opt/spark-job/
CMD cd /opt/spark-job/ && bin/submit-job.sh --local-mode --master spark://127.0.0.1:7077 --total-executor-cores 1 -b -m spark://127.0.0.1:7077
