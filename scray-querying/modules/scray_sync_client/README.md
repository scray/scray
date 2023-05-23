### Quickstart

```bash
!pip uninstall scray_sync_client -y
!pip install git+https://github.com/scray/scray.git@python-client#subdirectory=scray-querying/modules/scray_sync_client
```

```python
from scray.client import ScrayClient
from scray.client.config import ScrayClientConfig

config = ScrayClientConfig(
    host_address = "scray.example.com",
    port = 8082
)
client = ScrayClient(client_config=config)

client.getLatestVersion('http://scray.org/sender/4711/')
```