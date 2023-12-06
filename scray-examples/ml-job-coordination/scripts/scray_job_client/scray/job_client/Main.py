from scray.job_client.client import ScrayJobClient 
from scray.job_client.config import ScrayJobClientConfig
from scray.job_client.models.cli_args  import ArgsParser

if __name__ == '__main__':

    config = ScrayJobClientConfig(
        host_address = "http://ml-integration.research.dev.seeburger.de",
        port = 8082
    )

    client = ScrayJobClient(config=config)
    client.setState(job_name="J45", processing_env="http://example.com/", docker_image="helloworld", source_data="ddd", notebook_name="nn", state="WANTED_D")



    client.wait_for_job_completion(job_name="J45")



