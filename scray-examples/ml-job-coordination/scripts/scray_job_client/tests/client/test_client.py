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
from scray.client.client import ScrayClient

from scray.client.config import ScrayClientConfig
from scray.job_client.client import ScrayJobClient
from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.conf.s3_configuration import S3Configuration
from scray.job_client.models.job_state_configuration import JobStates

logger = logging.getLogger(__name__)

class TestScrayClient(TestCase):
    def test_client(self):
        
        config = ScrayClientConfig(
            host_address = "scray.example.com",
            port = 8082
        )

        client = ScrayClient(client_config=config)
        version = client.getLatestVersion

        self.assertEqual(version, 1)

    def test_agent_conf_loading(self):
        config = ScrayClientConfig(
            host_address = "scray.example.com",
            port = 8082
        )

        client = ScrayJobClient(config=config)

        agent_conf = client.get_agent_conf(env = "env1", agent_name = "process")

        name = "agent-007"
        env = "http://scray.org/sync/agent/configuration"
        states = [JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])]
        input  = S3Configuration(hostname = "https://s3.example.com", bucket = "data-bucket", path = "/in-data/data.txt")
        output = S3Configuration(hostname = "https://s3.example.com", bucket = "data-bucket", path = "/out-data/")

        agent_conf = AgentConfiguration(env=env, name=name, job_states=states, data_input_conf=input, data_output_conf=output)

        client.set_agent_conf(self, env = "env1", agent_name = "process", agent_conf=agent_conf)

if __name__ == '__main__':
    unittest.main()