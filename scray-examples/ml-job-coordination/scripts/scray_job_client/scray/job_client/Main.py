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
    job_state = client.setState(job_name="doerners_super_docs_4712", processing_env="http://scray.org/ai/app/env/see/os/k8s", state="READY_TO_CONVERT", metadata=metadata)

    print(client.get_job_metadata("doerners_super_docs_4712"))



