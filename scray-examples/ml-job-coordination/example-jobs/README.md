
### Prerequisites

### Software packages 

  * ```jq, sftp, curl```

### Configuration

Add client public key to ```authorized keys``` of data integration server.

## Submit job
```
./run-on-ki1.sh run\
  --job-name timestamp-example\
  --notebook-name timestamp-example.ipynb\
  --processing-env http://scray.org/ai/jobs/env/see/ki1-k8s
```


