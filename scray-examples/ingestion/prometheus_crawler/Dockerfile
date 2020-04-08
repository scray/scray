FROM openjdk

MAINTAINER Author Stefan Obermeier <st.obermeier@seeburger.de>

ENV PROMETHEUS_METRIKLIST_URL="http://127.0.0.1/prometheus/api/v1"
ENV DESTINATION_PATH="hdfs://127.0.1/user/hdfs/prometheus_data/qa_prometheus_single_metrics_"
ENV SYNC_PATH="hdfs://127.0.0.1/user/hdfs/prometheus_data/sync"
 
ADD target/prometheus_crawler-1.0-SNAPSHOT-jar-with-dependencies.jar /prometheus_crawler.jar
  
CMD /usr/bin/java -jar /prometheus_crawler.jar ${PROMETHEUS_METRIKLIST_URL} ${DESTINATION_PATH} ${SYNC_PATH}
