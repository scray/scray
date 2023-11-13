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


if __name__ == '__main__':
    unittest.main()