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
from scray.client.models.versioned_data import VersionedData
from scray.client import ScrayClient

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


    def setState(self, state, job_name, processing_env, docker_image, source_data, notebook_name):

        data = json.dumps({
                "filename": f"{job_name}.tar.gz",
                "processingEnv": processing_env,
                "state": state,
                "imageName": docker_image,
                "dataDir": source_data,
                "notebookName": notebook_name
            })

        versionedData = VersionedData(
                                    data_source = job_name,
                                    merge_key = "_",
                                    version = 0,
                                    data= data,
                                    version_key = 0)

        self.client.updateVersion(versionedData)

