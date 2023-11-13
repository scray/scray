from scray.job_client import ScrayJobClient
from scray.job_client.config import ScrayJobClientConfig

config = ScrayJobClientConfig(
    host_address = "http://ml-integration.research.dev.seeburger.de",
    port = 8082
)
client = ScrayJobClient(client_config=config)

latestVersion = client.getLatestVersion('_', 'm4t2-19536')

client.updateVersion(latestVersion)

print(latestVersion)

