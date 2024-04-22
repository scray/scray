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
from scray.client.models.versioned_data import VersionedData

from requests import Session

logger = logging.getLogger(__name__)

class ScraySyncFileClient:
    
    def __init__(
        self,
        filename: str,
        logging_level: Optional[int] = logging.DEBUG,
    ):
        self.logging_level = logging_level
        self.filename = filename

        self.request_session = Session()


    def create_versioned_data_object(self, versioned_data_dic):
        return VersionedData().fromDict(versioned_data_dic)
    
    def load_versioned_data_from_file(self):

        try:
            with open(self.filename, 'r') as sync_file:
                return list(map(self.create_versioned_data_object,json.load(sync_file)))
        except FileNotFoundError:
            logger.error(f"Error: The file '{self.filename}' does not exist.")
        except json.JSONDecodeError:
            logger.error("Error: Failed to decode JSON. The file may be corrupted or incorrectly formatted.")
