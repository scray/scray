# To run the full setup, please execute the following:

## Kubernetes
```
kubectl create -f k8s-deployment.yaml
```
or

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/k8s-deployment.yaml
```

# Data location

### MongoDB
 * hostname: mongodb-host
 * port: 30081 
 * db: "nrw"
 * collection: traffic
 
### MongoDB writer REST-API
 * port: 30080
 * Path to api descripton: /scray/examples/1.0.0/swagger.json
