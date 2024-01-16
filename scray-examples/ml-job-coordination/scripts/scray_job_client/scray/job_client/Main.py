from scray.job_client.client import ScrayJobClient
from scray.job_client.config import ScrayJobClientConfig
import json

if __name__ == '__main__':

    config = ScrayJobClientConfig(
    host_address = "http://ml-integration.research.dev.seeburger.de",
    port = 8082
    )


    metadata  = {
    "url": "example.com/chicken",
    "price": 100,
    "description": "This is a sample advertisement."
}

    client = ScrayJobClient(config=config)
    job_state = client.get_jobs(processing_env="http://scray.org/ai/app/env/see/os/k8s", requested_state="TEST")

    print("FFF....... " + str(job_state))



