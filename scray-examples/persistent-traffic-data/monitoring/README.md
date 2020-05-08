# To run the full setup, please execute the following:

## Kubernetes

```
kubectl apply -f https://raw.githubusercontent.com/scray/scray/feature/ingestion-examples/scray-examples/persistent-traffic-data/monitoring/k8s-deployment.yaml
```

### External Ports
 * Grafana: 30300
 * Prometheus: 30090
 
### DNS name
 * ```scray-example-monitoring```
 
 ### Access dashboard
 [http://localhost:30300/d/8ec0OC6Wz/scray-example-prometheus?orgId=1](http://localhost:30300/d/8ec0OC6Wz/scray-example-prometheus?orgId=1)
