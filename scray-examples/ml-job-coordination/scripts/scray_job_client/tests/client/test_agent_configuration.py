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

import json
from unittest import TestCase
import unittest
import logging

from scray.job_client.models.agent_configuration import AgentConfiguration
from scray.job_client.models.conf.s3_configuration import S3Configuration
from scray.job_client.models.conf.scray_job_metadata_configuration import ScrayJobMetadataConfiguration
from scray.job_client.models.job_state_configuration import JobStates

logger = logging.getLogger(__name__)

class TestAgentConfiguration(TestCase):

    def test_job_state_configuration(self):

        name = "agent-007"
        env = "http://scray.org/sync/agent/configuration"
        states = [
            JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"]),
            JobStates(env="http://scray.org/ai/jobs/env/see/001", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])
        ]

        input  = S3Configuration(hostname = "https://s3.example.com", bucket = "data-bucket", path = "/in-data/data.txt")
        output = S3Configuration(hostname = "https://s3.example.com", bucket = "data-bucket", path = "/out-data/")


        agent_conf = AgentConfiguration(env=env, name=name, job_states=states, data_input_conf=input, data_output_conf=output)

        
        self.assertEqual(agent_conf.env, env)
        self.assertEqual(len(agent_conf.job_states), 2)
        self.assertIsInstance(agent_conf.data_input_conf, S3Configuration)
        self.assertEqual(agent_conf.data_input_conf.bucket,"data-bucket")

    def test_job_state_configuration_serialization(self):

        name = "agent-007"
        env = "http://scray.org/sync/agent/configuration"
        states = [
            JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"]),
            JobStates(env="http://scray.org/ai/jobs/env/see/001", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])
        ]


        input  = S3Configuration(hostname = "https://s3.i1.example.com", bucket = "data-bucket", path = "/in-data/data.txt", data_description = "open api description")
        
        description = """
        {   
          "requestBody": {
              "content": {},
              "required": true
          }
        }
        """
        output = S3Configuration(hostname = "https://s3.i2.example.com", bucket = "data-bucket", path = "/out-data/", 
                                 data_description = description)

        
        agent_conf = AgentConfiguration(env=env, name=name, job_states=states, data_input_conf=input, data_output_conf=output)

        conf_json = json.dumps(agent_conf.to_dict(), indent=4)
        
        # Check produced json
        self.assertTrue("\"hostname\": \"https://s3.i1.example.com\"" in str(conf_json))
        self.assertTrue("\"hostname\": \"https://s3.i2.example.com\"" in str(conf_json))

        print(conf_json)

        # Deserialize configuration
        deserialized_conf = AgentConfiguration.from_json(conf_json)

        # Check object created from json data
        self.assertEqual(deserialized_conf.data_input_conf.hostname, "https://s3.i1.example.com")
        self.assertEqual(deserialized_conf.data_output_conf.hostname, "https://s3.i2.example.com")

    def test_job_state_configuration_serialization_job_metada(self):

            name = "agent-007"
            env = "http://scray.org/sync/agent/configuration"
            states = [
                JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"]),
                JobStates(env="http://scray.org/ai/jobs/env/see/001", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])
            ]

            input  = ScrayJobMetadataConfiguration(hostname = "https://s3.i5.example.com", env="http://scray.org/ai/jobs/env/see/000/configuration", jobname = "job_4711", data_description = "open api description")
            output  = ScrayJobMetadataConfiguration(hostname = "https://s3.i8.example.com", env="http://scray.org/ai/jobs/env/see/001/configuration", jobname = "job_4712", data_description = "open api description")

            
            agent_conf = AgentConfiguration(env=env, name=name, job_states=states, data_input_conf=input, data_output_conf=output)

            conf_json = json.dumps(agent_conf.to_dict(), indent=4)
            
            # Check produced json
            self.assertTrue("\"hostname\": \"https://s3.i5.example.com\"" in str(conf_json))
            self.assertTrue("\"hostname\": \"https://s3.i8.example.com\"" in str(conf_json))

            print(conf_json)

            # Deserialize configuration
            deserialized_conf = AgentConfiguration.from_json(conf_json)

            # Check object created from json data
            self.assertEqual(deserialized_conf.data_input_conf.hostname, "https://s3.i5.example.com")
            self.assertEqual(deserialized_conf.data_output_conf.hostname, "https://s3.i8.example.com")

if __name__ == '__main__':
    unittest.main()