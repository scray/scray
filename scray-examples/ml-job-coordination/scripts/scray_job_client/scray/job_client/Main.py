from scray.job_client.client import ScrayJobClient
from scray.job_client.config import ScrayJobClientConfig
from scray.job_client.models.cli_args import ArgsParser
import json

# if __name__ == '__main__':


def main_cli():
    args_parser = ArgsParser()
    job_args = args_parser.parse_args()

    print(f"Job Name: {job_args.job_name}")
    print(f"Source Data: {job_args.source_data}")
    print(f"Notebook Name: {job_args.notebook_name}")
    print(f"Initial State: {job_args.initial_state}")
    print(f"Processing Environment: {job_args.processing_env}")
    print(f"Docker Image: {job_args.docker_image}")
    print(f"Take Job Name Literally: {job_args.job_name_literally}")

    

