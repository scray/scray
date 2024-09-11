import argparse
from scray.job_client.models.job_arguments import JobArguments

class ArgsParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser(description="Command Line Argument Parser")      

        self.parser.add_argument("run", help='Define which context is used (e.g., run)')
        self.parser.add_argument('--job-name', dest='job_name', help='Specify job name')
        self.parser.add_argument('--source-data', dest='source_data', help='Specify source data')
        self.parser.add_argument('--notebook-name', dest='notebook_name', help='Specify notebook name', required=False)
        self.parser.add_argument('--initial-state', dest='initial_state', help='Specify initial state')
        self.parser.add_argument('--processing-env', dest='processing_env', help='Specify processing environment')
        self.parser.add_argument('--docker-image', dest='docker_image', help='Specify Docker image')
        self.parser.add_argument('--take-jobname-literally', dest='job_name_literally', help='Specify whether to take job name literally')

    def parse_args(self):
        args = self.parser.parse_args()


        # Ensure that the context is 'run' before allowing other arguments
        if not args.run:
            print("Error: This command only supports the 'run' context.")
            self.parser.print_help()
            exit(1)

        # Validate that required arguments for 'run' are provided
        if not args.notebook_name:
            print("Error: '--notebook-name' is required when context is 'run'.")
            exit(1)

        job_args = JobArguments()
        job_args.job_name = args.job_name
        job_args.source_data = args.source_data
        job_args.notebook_name = args.notebook_name
        job_args.initial_state = args.initial_state
        job_args.processing_env = args.processing_env
        job_args.docker_image = args.docker_image
        job_args.job_name_literally = args.job_name_literally
        return job_args
