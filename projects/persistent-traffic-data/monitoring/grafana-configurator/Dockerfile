FROM curlimages/curl:7.70.0

RUN mkdir /home/curl_user/grafana-configurator
 
COPY ./config.sh /home/curl_user/grafana-configurator/
COPY ./configure-grafana.sh /home/curl_user/grafana-configurator/
  
CMD sleep 3 && cd /home/curl_user/grafana-configurator/ && ./configure-grafana.sh && sleep 3000000
