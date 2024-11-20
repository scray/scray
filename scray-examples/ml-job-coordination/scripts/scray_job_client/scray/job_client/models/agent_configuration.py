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

from dataclasses import dataclass
import json
from typing import List

from scray.job_client.models.conf.agent_data_io_configuration import AgentDataIoConfiguration
from scray.job_client.models.job_state_configuration import JobStates

@dataclass
class AgentConfiguration:
    env: str
    name: str
    job_states: List[JobStates]
    data_input_conf: AgentDataIoConfiguration
    data_output_conf: AgentDataIoConfiguration

    @staticmethod
    def from_json(json_string):
        instance = AgentConfiguration()

        data = json.loads(json_string)
        instance.filename = data.get('filename')
        instance.state = data.get('state')
        instance.dataDir = data.get('dataDir')
        instance.notebookName = data.get('notebookName')
        instance.state = data.get('state')
        instance.imageName = data.get('imageName')
        instance.processingEnv = data.get('processingEnv')
        instance.metadata = data.get('metadata')

        return instance


