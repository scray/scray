To run the server, please execute the following:

```
mvn clean package jetty:run
```

You can then view the OpenAPI v2 specification here:

```
http://localhost:8080/hyperledger-fabric-invoice-example/1.0.0/swagger.json
```

```
curl -X POST "http://localhost:8080/hyperledger-fabric-invoice-example/1.0.0/invoice/123" -H  "accept: application/json" -H  "Content-Type: application/json" -d "{  \"id\": \"d290f1ee-6c54-4b01-90e6-d701748f0851\",  \"total\": 1.5,  \"state\": \"send\",  \"date\": 1600181844}"
```

```
curl -X GET "http://localhost:8080/hyperledger-fabric-invoice-example/1.0.0/invoice/123" -H  "accept: application/json"
```

# Configuration
	* Paht of file: config/config.yaml
	

	```docker build -t hyperleder-fabric-invoice-example-rest-api .```
	## Run in Docker container
	
	```docker run  --mount type=bind,source=/home/stefan/Dokumente/hyperleder-fabric/,target=/mnt/config/hyperleder-fabric --network=host hyperleder-fabric-invoice-example-rest-api```
	
	## Run in Kubernetes cluster
	kubectl apply k8s.yaml
	kubectl cp /home/stefan/Dokumente/hyperleder-fabric/ default/hyperleder-fabric-rest-invoice-example:/mnt/config/hyperleder-fabric/