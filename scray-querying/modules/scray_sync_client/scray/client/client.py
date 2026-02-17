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
from scray.client.models.http_client import HttpClient
import json
from urllib.parse import urlsplit, urlencode

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
        self.httpClient = HttpClient(token_provider=lambda: client_config.client_secret)

    def create() -> None: logger.info("Create scray client")

    def createUrl(self, api_subpath: str) -> str:
        host_address = self.client_config.host_address
        if "://" not in host_address:
            host_address = "https://" + host_address

        parts = urlsplit(host_address)

        base_path = parts.path.rstrip("/")
        api_subpath = "/" + api_subpath.lstrip("/")

        return f"{parts.scheme}://{parts.hostname}:{self.client_config.port}{base_path}{api_subpath}"


    def getLatestVersion(self, datasource, mergeky) -> VersionedData:

        url = self.createUrl(f"sync/versioneddata/latest?datasource={datasource}&mergekey={mergeky}")
        logger.debug("Request " + url)
        response = self.httpClient.get(conn=self.request_session, method="GET", url=url)

        result = VersionedData()
        result.fromDict(response)

        return result
    
    def getLatestVersionedDataByState(self, env, state) -> list[VersionedData]:

        url = self.createUrl("/indexes/state-env/search/")

        logger.debug("Request " + url)

        filter_str = f"data.processingEnv=={env};data.state=={state}"
        payload = {
            "filter": filter_str
        }

        response = self.httpClient.post(
            conn=self.request_session,
            url=url,
            data=json.dumps(payload)
        )

        def create_versioned_data_object(response):
            if response is None:
                return []
            
            else:
                result = []
                for item in response:
                    obj = VersionedData()
                    obj.fromDict(item)
                    result.append(obj)
                return result
            result = VersionedData()
            result.fromDict(response)
            return result

        return create_versioned_data_object(response)
    

    def get_all_versioned_data(self) -> list[VersionedData]:

        url = self.createUrl("sync/versioneddata/all/latest/")

        logger.debug("Request " + url)
        response = self.httpClient.get(conn=self.request_session, method="GET", url=url)

        if response is None:
            return []
        if not isinstance(response, list):
            raise TypeError(f"Expected list, got {type(response)}")

        result: list[VersionedData] = []
        for item in response:
            vd = VersionedData()
            vd.fromDict(item)
            result.append(vd)
        return result

    def updateVersion(self, versionedData):
        url = self.createUrl(f"/sync/versioneddata/latest/?datasource={versionedData.data_source}&mergekey={versionedData.merge_key}")
        
        logger.debug("Request " + url)

        self.httpClient.put(conn=self.request_session, url=url, data=versionedData.to_api_json())
