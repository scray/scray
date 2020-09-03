To run the server, please execute the following:

```
mvn clean package jetty:run
```

You can then view the swagger listing here:

```
curl -X POST "http://virtserver.swaggerhub.com/obermeier5/hyperledger_fabric_invoice_example/1.0.0/invoice/123" -H  "accept: application/json" -H  "Content-Type: application/json" -d "{  \"id\": \"d290f1ee-6c54-4b01-90e6-d701748f0851\",  \"total\": 1.5,  \"state\": \"send\",  \"date\": {}}"
```

```
curl -X GET "http://virtserver.swaggerhub.com/obermeier5/hyperledger_fabric_invoice_example/1.0.0/invoice/111" -H  "accept: application/json"
```