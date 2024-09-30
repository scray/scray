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
from scray.job_client.models.job_sync_api_data import JobSyncApiData
from scray.client.models.versioned_data import VersionedData
from scray.client import ScrayClient
from scray.job_client.io import create_archive
import time

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

            time.sleep(1)
    
    def wait_for_job_state(self, job_name, desiredState):
        
        while True:
            
            latestVersion = self.client.getLatestVersion

            logger.info("Latest version data: " + latestVersion.to_str())

            state = JobSyncApiData.from_json(json_string=latestVersion.data).state

            print(f"Waiting for state '{desiredState}'; current state is '{state}'")

            if state == desiredState:
                print(f"State '{desiredState}' reached")
                break

            time.sleep(1)

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
    




    def wait_for_job_state(self, job_name, desiredState):
        
        while True:
            
            latestVersion = self.client.getLatestVersion('_', job_name)

            logger.info("Latest version data: " + latestVersion.to_str())

            state = JobSyncApiData.from_json(json_string=latestVersion.data).state

            print(f"Waiting for state '{desiredState}'; current state is '{state}'")

            if state == desiredState:
                print(f"State '{desiredState}' reached")
                break

            time.sleep(1)


    def setState(self, state, job_name, processing_env, docker_image = "_", source_data = "_", notebook_name = "_", metadata = ""):

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

    def get_job_metadata(self, job_name):
        
        latestVersion = self.client.getLatestVersion('_', job_name)
        logger.info("Latest version data: " + latestVersion.to_str())
        metadata = JobSyncApiData.from_json(json_string=latestVersion.data).metadata

        return metadata
    
    def upload_notebook(self, job_name, source_data ):
        create_archive(job_name, source_data, self.config, data_integration_host)

