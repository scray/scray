#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
from typing import Dict, Optional
import json
from scray.client.config import ScrayClientConfig
from scray.job_client.config import ScrayJobClientConfig
from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.job_state_configuration import JobStates
from scray.job_client.models.job_sync_api_data import JobSyncApiData
from scray.client.models.versioned_data import VersionedData
from scray.client import ScrayClient
from scray.job_client.io import create_archive
import time
import uuid

from requests import Session

logger = logging.getLogger(__name__)


class ScrayJobClient:
    client = None

    def __init__(
        self,
        config: ScrayJobClientConfig,
        logging_level: Optional[int] = logging.DEBUG,
    ):
        self.logging_level = logging_level
        self.config = config
       # self.client_config = client_config

        scrayClientConfig = ScrayClientConfig(

            host_address = config.host_address,
            port = config.port
        )

        self.client = ScrayClient(client_config=scrayClientConfig)


    def __waitForJobcompletion():  {

    }
        
    def load_job_state_configuration(self, processor = "42") -> JobStates:
        trigger_states = [("env1", "UPLOADED")]
        error_states = [("env1", "CONVERSION_ERROR")]
        completed_states =  [("env1", "CONVERTED")]

        return JobStates(trigger_states=trigger_states, error_states=error_states, completed_states=completed_states)


    def get_job_state(self, job_name):
        
        latestVersion = self.client.getLatestVersion('_', job_name)
        logger.info("Latest version data: " + latestVersion.to_str())
        job_state = JobSyncApiData.from_json(json_string=latestVersion.data).state

        return job_state

    def wait_for_job_completion(self, job_name):
        
        while True:
            
            latestVersion = self.client.getLatestVersion('_', job_name)

            logger.info("Latest version data: " + latestVersion.to_str())

            state = JobSyncApiData.from_json(json_string=latestVersion.data).state

            print(f"Waiting for state 'COMPLETED'; current state is '{state}'")

            if state == "COMPLETED":
                print("State 'COMPLETED' reached")
                break

            time.sleep(3)
    
    def wait_for_job_state(self, job_name, desiredState):
        
        while True:
            
            latestVersion = self.client.getLatestVersion

            logger.info("Latest version data: " + latestVersion.to_str())

            state = JobSyncApiData.from_json(json_string=latestVersion.data).state

            print(f"Waiting for state '{desiredState}'; current state is '{state}'")

            if state == desiredState:
                print(f"State '{desiredState}' reached")
                break

            time.sleep(3)

    def get_jobs(self, processing_env, requested_state=None) -> list[str]:
              
            latestVersions = self.client.get_all_versioned_data()
            if latestVersions is None:
                logger.info("No new version available")
                return []
            else:
                logger.info("Latest version data: " + str(type(latestVersions)))

                def env_state_filter(latestVersion) -> str:
                    try:
                        metadata = JobSyncApiData.from_json(json_string=latestVersion.data)

                        if metadata.processingEnv == processing_env:
                            if requested_state is None:
                                return True  # If no state is requested, include all states
                            else:
                                return metadata.state == requested_state
                        else:
                            return False
                        
                    except ValueError:
                        return False 

                
                def get_job_name(versioned_data) -> str:
                    return versioned_data.data_source
                
                job_with_matching_state = list(filter(env_state_filter, latestVersions))

                return list(map(get_job_name, job_with_matching_state))
    

    def wait_for_new_job(self, processing_env, requested_state)-> list[str]:

        while True:

            jobs = self.get_jobs(processing_env, requested_state)

            if jobs:
                return jobs
                
            time.sleep(1)

    def setState(self, state, job_name, processing_env, docker_image = "scrayorg/scray-jupyter_tensorflow-gpu:0.1.3", source_data = "./", notebook_name = "token_classification_01.ipynb", metadata = ""):

        data = json.dumps({
                "filename": f"{job_name}.tar.gz",
                "processingEnv": processing_env,
                "state": state,
                "imageName": docker_image,
                "dataDir": source_data,
                "notebookName": notebook_name,
                "metadata": metadata
            })

        versionedData = VersionedData(
                                    data_source = job_name,
                                    merge_key = "_",
                                    version = 0,
                                    data= data,
                                    version_key = 0)

        self.client.updateVersion(versionedData)

    def get_agent_conf(self, env: str, agent_name: str) -> AgentConfiguration:
        
        latestVersion = self.client.getLatestVersion(agent_name, '_')
        logger.info("Latest agent config version: " + latestVersion.to_str())
        agent_conf = AgentConfiguration.from_json(json_string=latestVersion.data)

        return agent_conf
    
    def set_agent_conf(self, env: str, agent_name: str, configuration: AgentConfiguration):

        conf_json = json.dumps(configuration.to_dict())

        versionedData = VersionedData(
                                    data_source = agent_name,
                                    merge_key = "_",
                                    version = 0,
                                    data= conf_json,
                                    version_key = 0)

        self.client.updateVersion(versionedData)


    def get_job_metadata(self, job_name):
        
        latestVersion = self.client.getLatestVersion('_', job_name)
        logger.info("Latest version data: " + latestVersion.to_str())
        metadata = JobSyncApiData.from_json(json_string=latestVersion.data).metadata

        return metadata
    env = ""


    def upload_job(self, source_data, notebook_name: str,  processing_env: str = "http://scray.org/ai/jobs/env/see/ki1-k8s", job_name = "job-" + str(uuid.uuid4())):
        create_archive(job_name, source_data, self.config.data_integration_user, self.config.data_integration_host)

        return job_name

    def deploy_job(self, source_data, notebook_name: str,  processing_env: str = "http://scray.org/ai/jobs/env/see/ki1-k8s", job_name = "job-" + str(uuid.uuid4()), initState="UPLOADED", docker_image="scray/python:0.1.3", metadata = ""):
        create_archive(job_name, source_data, self.config.data_integration_user, self.config.data_integration_host)
        self.setState(state=initState, 
                job_name=job_name, 
                processing_env=processing_env, 
                notebook_name=notebook_name,
                docker_image=docker_image,
                metadata=metadata
            )
        
        return job_name
    
    def get_job_fin_data(job_name, destination_path, data_integration_user, data_integration_host):
        """
        Downloads the completed job data and extracts it to a specified destination.

        :param job_name: Name of the job (used for the file name).
        :param destination_path: Path where the extracted files should be stored.
        :param data_integration_user: Username for the SFTP connection.
        :param data_integration_host: Host of the SFTP server.
        """
        temp_tar_path = f"/tmp/{job_name}.tar.gz"
        
        # Ensure the destination path exists
        os.makedirs(destination_path, exist_ok=True)

        transport = paramiko.Transport((data_integration_host, 22))
        
        try:
            private_key_path = f"{Path.home()}/.ssh/id_rsa"
            key = paramiko.RSAKey.from_private_key_file(private_key_path)

            # Connect to SFTP
            transport.connect(username=data_integration_user, pkey=key)
            sftp = paramiko.SFTPClient.from_transport(transport)
            
            # Download the archive
            remote_path = f"sftp-share/{job_name}.tar.gz"
            print(f"Downloading {remote_path} to {temp_tar_path}")
            sftp.get(remote_path, temp_tar_path)
            sftp.close()

            # Extract the archive
            with tarfile.open(temp_tar_path, "r:gz") as tar:
                tar.extractall(path=destination_path)
                print(f"Extracted {job_name}.tar.gz to {destination_path}")

        except Exception as e:
            print(f"Error during download and extraction: {e}")
        finally:
            transport.close()
            
            # Remove the downloaded archive after extraction
            if os.path.exists(temp_tar_path):
                os.remove(temp_tar_path)
                print(f"Removed temporary file {temp_tar_path}")

