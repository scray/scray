from scray.job_client.client import ScrayJobClient
from scray.job_client.config import ScrayJobClientConfig
from scray.job_client.models.job_arguments  import ArgsParser

if __name__ == '__main__':
    args_parser = ArgsParser()
    job_args = args_parser.parse_args()

    # Now you can use the JobArguments instance (job_args) in your program
    print(f"Job Name: {job_args.job_name}")
    print(f"Source Data: {job_args.source_data}")
    print(f"Notebook Name: {job_args.notebook_name}")
    print(f"Initial State: {job_args.initial_state}")
    print(f"Processing Environment: {job_args.processing_env}")
    print(f"Docker Image: {job_args.docker_image}")
    print(f"Job Name Literally: {job_args.job_name_literally}")


config = ScrayJobClientConfig(
    host_address = "http://ml-integration.research.dev.seeburger.de",
    port = 8082
)
client = ScrayJobClient(client_config=config)

latestVersion = client.getLatestVersion('_', 'm4t2-19536')

client.updateVersion(latestVersion)

print(latestVersion)

