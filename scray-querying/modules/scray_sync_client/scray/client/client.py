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

class ScrayClient:

    def __init__(
        self,
        client_config: ScrayClientConfig,
        logging_level: Optional[int] = logging.DEBUG,
    ):
        self.logging_level = logging_level
        self.client_config = client_config

        self.request_session = Session()

    
    def create() -> None: logger.info("Create scray client")

    def getLatestVersion(self, datasource, mergeky) -> VersionedData:

        url = f"{self.client_config.host_address}:{self.client_config.port}/sync/versioneddata/latest/?datasource={datasource}&mergekey={mergeky}"
        logger.debug("Request " + url)
        response = self._make_getrequest(conn=self.request_session, method="GET", url=url)

        result = VersionedData()
        result.fromDict(response)

        return result


    def updateVersion(self, versionedData):
        url = f"{self.client_config.host_address}:{self.client_config.port}/sync/versioneddata/latest/?datasource={versionedData.data_source}&mergekey={versionedData.merge_key}"
        logger.debug("Request " + url)

        self._make_putrequest(conn=self.request_session, url=url, data=versionedData.to_api_json)




    def _make_getrequest(
        self, conn, method, url
    ):

        response = conn.request(method, url)
        if response.status_code == 200:
            return response.json()
        else:
            logger.error(f"Error while interacting with sync API. Code: {response.status_code}")
            return ""
    
    def _make_putrequest(
        self, conn, url, data
    ):
        newHeaders = {'Content-type': 'application/json'}

        response = conn.put(url, data=str(data()), headers=newHeaders)
        if response.status_code == 200:
            logger.info("State successfully updated")
        else:
            logger.error(f"Error while interacting with sync API. Code: {response.status_code}")