import logging
import os
from pathlib import Path
import paramiko
import tarfile

logger = logging.getLogger(__name__)

def download_updated_notebook(job_name, notebook_name, data_integration_user, data_integration_host):
    # Remove existing tar.gz file if it exists
    tar_gz_file = f"{job_name}-state.tar.gz"
    if os.path.exists(tar_gz_file):
        os.remove(tar_gz_file)
    
    # Download the file using SFTP
    transport = paramiko.Transport((data_integration_host, 22))
    try:
        transport.connect(username=data_integration_user)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(f"sftp-share/{tar_gz_file}", tar_gz_file)
        sftp.close()
    except Exception as e:
        print(f"Error during SFTP transfer: {e}")
        return
    finally:
        transport.close()
    
    # Extract the tar.gz file if it was successfully downloaded
    if os.path.exists(tar_gz_file):
        try:
            with tarfile.open(tar_gz_file, "r:gz") as tar:
                tar.extractall()
            os.remove(tar_gz_file)
            print(f"Notebook {notebook_name} updated")
        except Exception as e:
            print(f"Error during tar extraction: {e}")


def create_archive(job_name, source_data, data_integration_user, data_integration_host):
    print(f"Create archive {job_name}.tar.gz from source {source_data}")

    # Create tar.gz archive
    with tarfile.open(f"{job_name}.tar.gz", "w:gz") as tar:
        tar.add(source_data, arcname=os.path.basename(source_data))

    # Upload the file using SFTP
    transport = paramiko.Transport((data_integration_host, 22))
    try:
        private_key_path = f"{Path.home()}/.ssh/id_rsa"
        key = paramiko.RSAKey.from_private_key_file(private_key_path)

        transport.connect(username=data_integration_user, pkey=key)
        sftp = paramiko.SFTPClient.from_transport(transport)
        print(f"{job_name}.tar.gz")
        if os.path.exists(f"{job_name}.tar.gz"):
            sftp.put(f"{job_name}.tar.gz", f"sftp-share/{job_name}.tar.gz")
            sftp.close()
        else:
            logger.warning(f"Error: {job_name}.tar.gz does not exist!")
    except Exception as e:
        print(f"Error during SFTP transfer: {e}")
    finally:
        transport.close()
    
    # Remove the local tar.gz file
    os.remove(f"{job_name}.tar.gz")
    
    return job_name

def clean_up(job_name, notebook_name):
    # Remove old files
    files_to_remove = [
        f"{job_name}-fin.tar.gz",
        f"{job_name}.tar.gz",
        f"{job_name}-state.tar.gz",
        f"SYS-JOB-NAME-{job_name}.json",
        f"out.{notebook_name}"
    ]

    for file in files_to_remove:
        if os.path.exists(file):
            os.remove(file)

def download_results(job_name, data_integration_user, data_integration_host):
    # Remove existing fin tar.gz file if it exists
    fin_tar_gz_file = f"{job_name}-fin.tar.gz"
    if os.path.exists(fin_tar_gz_file):
        os.remove(fin_tar_gz_file)
    
    # Download the fin tar.gz file using SFTP
    transport = paramiko.Transport((data_integration_host, 22))
    try:
        transport.connect(username=data_integration_user)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get(f"sftp-share/{fin_tar_gz_file}", fin_tar_gz_file)
        sftp.close()
    except Exception as e:
        print(f"Error during SFTP transfer: {e}")
        return
    finally:
        transport.close()
    
    # Extract the tar.gz file if it was successfully downloaded
    if os.path.exists(fin_tar_gz_file):
        try:
            with tarfile.open(fin_tar_gz_file, "r:gz") as tar:
                tar.extractall()
            # Clean up
            os.remove(fin_tar_gz_file)
            os.remove(f"{job_name}.tar.gz")
            os.remove(f"{job_name}-state.tar.gz")
            os.remove(f"SYS-JOB-NAME-{job_name}.json")
            print("Learning results loaded")
        except Exception as e:
            print(f"Error during tar extraction: {e}")

    # Example usage
    create_archive(
        job_name="example_job",
        source_data="/path/to/source_data",
        data_integration_user="user",
        data_integration_host="host.com"
    )

    clean_up(
        job_name="example_job",
        notebook_name="example_notebook"
    )

    download_results(
        job_name="example_job",
        data_integration_user="user",
        data_integration_host="host.com"
    )

    # Example usage
    download_updated_notebook(
        job_name="example_job",
        notebook_name="example_notebook",
        data_integration_user="user",
        data_integration_host="host.com"
    )

    create_archive(
        job_name="example_job",
        source_data="/path/to/source_data",
        data_integration_user="user",
        data_integration_host="host.com"
    )

    clean_up(
        job_name="example_job",
        notebook_name="example_notebook"
    )

    download_results(
        job_name="example_job",
        data_integration_user="user",
        data_integration_host="host.com"
    )
