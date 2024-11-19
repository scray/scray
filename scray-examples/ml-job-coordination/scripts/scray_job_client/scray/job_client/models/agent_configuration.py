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

from typing import List

from scray.job_client.models.job_state_configuration import JobStates


class AgentConfiguration:

    """
    A class to represent the configuration of an agent.
    
    Attributes:
        env (str): The environment where the agent operates.
        name (str): The name of the agent.
        job_states (List[JobStates]): A list of job states related to the agent.
    """
    def __init__(self, env: str, name: str, job_states: List[JobStates]) -> None:
        self._env = env
        self._name = name
        self._job_states = job_states

    # Property for 'env'
    @property
    def env(self) -> str:
        return self._env

    @env.setter
    def env(self, value: str) -> None:
        if not isinstance(value, str):
            raise ValueError("Environment must be a string")
        self._env = value

    # Property for 'name'
    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Name must be a non-empty string")
        self._name = value

    # Property for 'job_states'
    @property
    def job_states(self) -> List[str]:
        return self._job_states

    @job_states.setter
    def job_states(self, states: List[str]) -> None:
        if not isinstance(states, list) or not all(isinstance(state, str) for state in states):
            raise ValueError("Job states must be a list of strings")
        self._job_states = states

    def __repr__(self) -> str:
        return (f"AgentConfiguration(env={self.env!r}, name={self.name!r}, "
                f"job_states={self.job_states!r})")

    def add_job_state(self, state: str) -> None:
        """Add a new job state to the list of job states."""
        if not isinstance(state, str):
            raise ValueError("Job state must be a string")
        self._job_states.append(state)

    def remove_job_state(self, state: str) -> None:
        """Remove a job state from the list of job states if it exists."""
        if state in self._job_states:
            self._job_states.remove(state)

