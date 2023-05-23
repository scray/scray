### Quickstart

```
!pip uninstall scray_sync_client -y
!pip install git+https://github.com/scray/scray.git@python-client#subdirectory=scray-querying/modules/scray_sync_client
```

```
from scray.client import ScrayClient
client = ScrayClient()

client.getLatestVersion('http://scray.org/sender/4711/')
```