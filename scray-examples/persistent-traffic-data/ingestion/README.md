# Example application:

This repository consists of components to analyse publicly available traffic data.
Three components can be deployed independently.

* [Ingestion component](ingestion-component)
    Traffic data are downloaded and stored in a MongoDB database
  
* Monitoring component
    Monitoring data are collected by Prometheus and visualized with Grafana

* Analytics component
    Componente to analyste the data with Python.
  


## Ingestion component
Traffic data are downloaded and stored in a MongoDB database

### Start ingestion component

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/k8s-deployment.yaml
```

### Data location

#### MongoDB
 * hostname: mongodb-host
 * port: 30081 
 * db: "nrw"
 * collection: traffic
 
#### MongoDB writer REST-API
 * port: 30080
 * Path to api descripton: /scray/examples/1.0.0/swagger.json

## Monitoring component
Monitoring data are collected by Prometheus and visualized with Grafana

### Start monitoring component

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/monitoring/k8s-deployment.yaml
```

#### External Ports
 * Grafana: 30300
 * Prometheus: 30090


## Analytics component
Jupyter environment to analyse the persited data.

### Start analytics component

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/analytics-workbench/k8s-jupyter-deployment.yaml
```

#### Jupyter Notebook
 * port: 30084
 * password: scray
 * GitHub repo: /mnt/scray

 
 
