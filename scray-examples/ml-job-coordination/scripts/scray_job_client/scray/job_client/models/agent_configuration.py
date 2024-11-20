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

from dataclasses import asdict, dataclass
import json
import logging
from typing import List

from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration
from scray.job_client.models.conf.scray_job_metadata_configuration import ScrayJobMetadataConfiguration
from scray.job_client.models.conf.s3_configuration import S3Configuration
from scray.job_client.models.job_state_configuration import JobStates

@dataclass
class AgentConfiguration:
    env: str
    name: str
    job_states: List[JobStates]
    data_input_conf: AgentDataIoConfiguration
    data_output_conf: AgentDataIoConfiguration

    logger = logging.getLogger(__name__)


    @staticmethod
    def _get_data_io_configuration(data: dict):
        if data.get("type") == "http://scray.org/agent/conf/io/type/s3":
            return(S3Configuration.from_dict(data))
        elif data.get("type") == "http://scray.org/agent/conf/io/type/scrayjobmetadata":
            return(ScrayJobMetadataConfiguration.from_dict(data))
        else:
            logging.error("Unknown io configuration type" + data.get("type", "No type defined"))
            return None


    @staticmethod
    def from_json(json_string):
        data = json.loads(json_string)

        instance = AgentConfiguration(
            env = data.get('env'),
            name = data.get('name'),
            job_states = data.get('job_states'),
            data_input_conf = AgentConfiguration._get_data_io_configuration(data.get('data_input_conf')),
            data_output_conf = AgentConfiguration._get_data_io_configuration(data.get('data_output_conf'))
        )

        return instance
    


    def to_dict(self):
        return {
            "env": self.env,
            "name": self.name,
            "job_states": [state.to_dict() for state in self.job_states],
            "data_input_conf": self.data_input_conf.to_dict(),
            "data_output_conf": self.data_output_conf.to_dict(),
        }


