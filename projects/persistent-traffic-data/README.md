# Highway traffic analytics:

This repository consists of components to analyse publicly available traffic data. All data are provided by [Landesbetrieb Straßenbau NRW, Verkehrszentrale](https://mcloud.de/web/guest/suche/-/results/suche/relevance/NRW/0/detail/_mcloudde_mdmgeschwindigkeitsdatennrw)  
Three components can be deployed independently.

* [Ingestion component](#ingestion-component)  
    Traffic data are downloaded and stored in a MongoDB database
  
* [Monitoring component](#monitoring-component)  
    Monitoring data are collected by Prometheus and visualized with Grafana

* [Analytics component](#analytics-component)  
    Componente to analyste the data with Python.
  
## Start all components
```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/ingestion/k8s-deployment.yaml -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/monitoring/k8s-deployment.yaml -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/analytics-workbench/k8s-jupyter-deployment.yaml
```
## Delete all components
```
kubectl delete -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/ingestion/k8s-deployment.yaml -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/monitoring/k8s-deployment.yaml -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/analytics-workbench/k8s-jupyter-deployment.yaml
```

## Ingestion component
Traffic data are downloaded and stored in a MongoDB database

### Start ingestion component
```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/k8s-deployment.yaml
```
[Doc of this component](ingestion/README.md)  

### Local links:  
[Api description](http://127.0.0.1:30091/scray/examples/1.0.0/swagger.json)   
 
## Monitoring component
Monitoring data are collected by Prometheus and visualized with Grafana

### Start monitoring component
```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/ingestion/monitoring/k8s-deployment.yaml
```
[Doc of this component](monitoring/README.md)

### Local links:  
[Grafana dashboard](http://localhost:30300/d/8ec0OC6Wz/scray-example-prometheus?orgId=1)  
[Promethues UI](http://localhost:30090)  

## Analytics component
Jupyter environment to analyse the persited data.

### Start analytics component
```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/analytics-workbench/k8s-jupyter-deployment.yaml
```
[Doc of this component](analytics-workbench/README.md)  

### Local links:  
[Example Jupyter notebook](http://127.0.0.1:30084/notebooks/work/Query%20NRW%20traffic%20MongoDB.ipynb)  


