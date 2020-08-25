
# Ingestion component
Traffic data are downloaded and stored in a MongoDB database

## Start ingestion component

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/master/projects/persistent-traffic-data/ingestion/k8s-deployment.yaml 
```

## Data location

### MongoDB
 * hostname: mongodb-host
 * port: 30081 
 * db: "nrw"
 * collection: traffic
 
### MongoDB writer REST-API
 * port: 30080
 * Path to api descripton: /scray/examples/1.0.0/swagger.json

