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
from unittest import TestCase
import unittest

from scray.job_client.models.job_state_configuration import JobStates
from scray.job_client.models.pipeline_creator import PipelineCreator


logger = logging.getLogger(__name__)

class TestScrayClient(TestCase):

    def testNodeToJsonSerialization(self):
        logger.info("Add node to pipeline")

        creator = PipelineCreator()

        name = "agent-007"
        start_state = JobStates(env="http://scray.org/ai/jobs/env/see/000", trigger_states=["UPLOADED"], error_states=["CONVERSION_ERROR"], completed_states=["SUMMARIZED"])
        node1 = creator.createNextNode(name, "jira-agent", start_state)

        self.assertTrue(creator.toJson(node1).find("trigger_states\": [\"SUMMARIZED\"]") != -1)

        logger.info("Serialize pipeline")

if __name__ == '__main__':
    unittest.main()