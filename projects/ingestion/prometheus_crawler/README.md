# Tool to query all prometheus data and store them in a SequenceFile.

## Run as docker container
  ```
  docker run \
    -e PROMETHEUS_METRIKLIST_URL='http://prometheus.example.com/prometheus/api/v1' \
    -e DESTINATION_PATH='hdfs://hdfs.example.com/user/hdfs/prometheus_data/prometheus_single_metrics_' \
    --name prometheus_crawler \
    scrayorg/prometheus-crawler
  ```
  
 ## Run in Kubernetes cluster
   Update `PROMETHEUS_METRIKLIST_URL` and `DESTINATION_PATH` in k8s-deployment.yaml
   
   Deploy pod
   ```
    kubectl apply -f k8s-deployment.yaml
   ```
