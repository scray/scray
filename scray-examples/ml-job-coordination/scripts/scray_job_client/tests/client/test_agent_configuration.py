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

from unittest import TestCase
import unittest
import logging

from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.job_state_configuration import JobStates

logger = logging.getLogger(__name__)

class TestAgentConfiguration(TestCase):
    def test_AgentConfiguration(self):

        name = "agent-007"
        env = "http://scray.org/sync/agent/configuration"
        states = [
            JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"]),
            JobStates(env="http://scray.org/ai/jobs/env/see/001", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])
        ]

        agent_conf = AgentConfiguration(env=env, name=name, job_states=states)

        
        self.assertEqual(agent_conf.env, env)
        self.assertEqual(len(agent_conf.job_states), 2)


if __name__ == '__main__':
    unittest.main()