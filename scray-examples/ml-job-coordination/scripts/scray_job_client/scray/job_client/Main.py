from scray.job_client.client import ScrayJobClient
from scray.job_client.config import ScrayJobClientConfig

if __name__ == '__main__':

    config = ScrayJobClientConfig(
    host_address = "http://ml-integration.research.dev.seeburger.de",
    port = 8082
    )

    client = ScrayJobClient(config=config)
    job_state = client.get_job_state("backend-16280")

    print(job_state)



