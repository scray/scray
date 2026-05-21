# Scray-Sync API

### Swagger UI
http://localhost:8082/swagger-ui/index.html



### Build from the repo root:

```
   docker build \
     -f scray-querying/modules/scray-sync-api-rest/Dockerfile \
     -t scray/sync-api-rest:dev \
     scray-querying
```

```
SECURITY_APITOKEN=eyJzdWIiOiIxMjM0...

docker run --rm -p 8082:8082 --env-file .env scray/sync-api-rest:dev
```