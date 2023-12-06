### Quickstart

```bash
!pip uninstall scray_job_client -y
!pip install git+https://github.com/scray/scray.git@python-client#subdirectory=scray-querying/modules/scray_sync_client
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