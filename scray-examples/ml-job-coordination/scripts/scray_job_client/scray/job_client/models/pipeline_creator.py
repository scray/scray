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
from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.job_state_configuration import JobStates


@dataclass
class AgentNode:
    name: str
    pip_package: str
    job_states: JobStates

    def to_dict(self):
        return {
            "name": self.name,
            "pip_package": self.pip_package,
            "job_states": self.prev_job_states.value  
        }


class PipelineCreator:
    
    """
        Create a pipeline node, connected to the previous node.
    """
    def createNextNode(self, name: str, pip_package_name: str, prev_job_states: JobStates) -> AgentNode:

        error_state = [name + "_ERROR"]
        completed_state = [name + "_COMPLETED"]
        job_states = JobStates(env=prev_job_states.env, trigger_states=prev_job_states.completed_states, error_states=error_state, completed_states=completed_state)

        return AgentNode(
            name=name, 
            pip_package=pip_package_name, 
            job_states=job_states
        )

    def toJson(self, node: AgentNode) -> str:
        return json.dumps(asdict(node))



