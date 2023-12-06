from scray.job_client.client import ScrayJobClient 
from scray.job_client.config import ScrayJobClientConfig
from scray.job_client.models.cli_args  import ArgsParser

if __name__ == '__main__':

    config = ScrayJobClientConfig(
    host_address = "http://ml-integration.research.dev.seeburger.de",
    port = 8082
    )

    client = ScrayJobClient(config=config)
    client.setState(job_name="backend-16280", processing_env="http://scray.org/ai/app/env/see/os/k8s ", state="WANTED_D")



