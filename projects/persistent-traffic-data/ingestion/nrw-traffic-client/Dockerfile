FROM openjdk

ADD target/nrw-traffic-client-0.0.1-SNAPSHOT-jar-with-dependencies.jar /nrw-traffic-client.jar
  
CMD /usr/bin/java -jar /nrw-traffic-client.jar ${PROMETHEUS_METRIKLIST_URL} ${DESTINATION_PATH} ${SYNC_PATH}
