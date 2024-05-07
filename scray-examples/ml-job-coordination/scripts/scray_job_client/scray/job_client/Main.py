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

    env = "http://scray.org/ai/app/env/see/os/k8s"

    #client.setState(state="TEST", job_name="test_job2", processing_env=env)
    job_state = client.get_jobs(processing_env=env, requested_state="CONVERTED")

    print("New job state is: ....... " + str(len(job_state)))




