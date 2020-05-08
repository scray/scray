FROM curlimages/curl:7.70.0

ENV HOME /home/aptly


RUN mkdir /home/curl_user/grafana-configurator
 
COPY ./config.sh /home/curl_user/grafana-configurator/
COPY ./configure-grafana.sh /home/curl_user/grafana-configurator/
  
CMD sleep 30000
