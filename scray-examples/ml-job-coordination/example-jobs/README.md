
### Prerequisites

### Software packages 

  * ```jq, sftp, curl```

### Configuration

Add client public key to ```authorized keys``` of data integration server.

## Submit job
```
export SCRAY_DATA_INTEGRATION_HOST=ml-integration.research.dev.example.de
export SCRAY_DATA_INTEGRATION_USER=ubuntu
export SCRAY_SYNC_API_URL=ml-integration.research.dev.example.de:8082


./run-on-ki1.sh run\
  --job-name timestamp-example\
  --notebook-name timestamp-example.ipynb\
  --processing-env http://scray.org/ai/jobs/env/see/ki1-k8s
```


