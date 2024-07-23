import argparse

def parse_args():
    parser = argparse.ArgumentParser(description='Parse command line arguments.')

    parser.add_argument('--job-name', type=str, help='Job name')
    parser.add_argument('--source-data', type=str, help='Source data')
    parser.add_argument('--notebook-name', type=str, help='Notebook name')
    parser.add_argument('--initial-state', type=str, help='Initial state')
    parser.add_argument('--processing-env', type=str, help='Processing environment')
    parser.add_argument('--docker-image', type=str, help='Docker image')
    parser.add_argument('--take-jobname-literally', type=str, help='Take job name literally')

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = parse_args()
    print(f"Job Name: {args.job_name}")
    print(f"Source Data: {args.source_data}")
    print(f"Notebook Name: {args.notebook_name}")
    print(f"Initial State: {args.initial_state}")
    print(f"Processing Environment: {args.processing_env}")
    print(f"Docker Image: {args.docker_image}")
    print(f"Take Job Name Literally: {args.take_jobname_literally}")
