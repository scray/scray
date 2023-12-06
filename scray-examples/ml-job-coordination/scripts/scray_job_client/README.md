### Install clients

```bash
!pip uninstall scray-sync-client -y
!pip install git+https://github.com/scray/scray.git@develop#subdirectory=scray-querying/modules/scray_sync_client

!pip uninstall scray-job-client -y
!pip install git+https://github.com/scray/scray.git@develop#subdirectory=scray-examples/ml-job-coordination/scripts/scray_job_client
```

### Kill job

```python
from scray.job_client.client import ScrayJobClient 
from scray.job_client.config import ScrayJobClientConfig


config = ScrayJobClientConfig(
  host_address = "http://ml-integration.research.dev.seeburger.de",
  port = 8082
)

client = ScrayJobClient(config=config)
client.setState(job_name="backend-16280", processing_env="http://scray.org/ai/app/env/see/os/k8s ", state="WANTED_D")
```

### Get job state

```python
from scray.job_client.client import ScrayJobClient 
from scray.job_client.config import ScrayJobClientConfig


config = ScrayJobClientConfig(
  host_address = "http://ml-integration.research.dev.seeburger.de",
  port = 8082
)

client = ScrayJobClient(config=config)
client.setState(job_name="backend-16280", processing_env="http://scray.org/ai/app/env/see/os/k8s ", state="WANTED_D")
```


