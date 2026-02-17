// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.scray.sync.analytics;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scray.sync.api.VersionedData;
import org.scray.sync.rest.SyncFileManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.impl.FileVersionedDataApiImpl;

public class MergeVersionFiles {
	private static final Logger logger = LoggerFactory.getLogger(MergeVersionFiles.class);

	public FileVersionedDataApiImpl removeState(List<String> statesToRemove, List<VersionedData> vData) {

		ObjectMapper objectMapper = new ObjectMapper();
		FileVersionedDataApiImpl statesToPersist = new FileVersionedDataApiImpl();

		for (VersionedData data : vData) {
			try {
				var state = objectMapper.readTree(data.getData());

				if(!statesToRemove.contains(state.get("state").asText())) {
					statesToPersist.updateVersion(data);
				}
			} catch (Exception e) {
				logger.warn("Error while parsing state attribute in version data");
			}
		}

		return statesToPersist;
	}

	public static void main(String[] args) {

		MergeVersionFiles remover = new MergeVersionFiles();
		List<VersionedData> vsData = new SyncFileManager("sync-api-stat.json").getSyncApi().getAllVersionedResources();
		List<VersionedData> vsData2 = new SyncFileManager("sync-api-stat.json.v2").getSyncApi().getAllVersionedResources();

		List<String> statesToRemove = Arrays.asList("COMPLETED", "CONVESION_ERROR", "ERROR", "PUBLISHED", "FINISHED", "LOADING_ERROR");


		List<VersionedData> allVersions = Stream.concat(vsData.stream(), vsData2.stream()).toList();

		FileVersionedDataApiImpl statesToPersist = remover.removeState(
				statesToRemove,
				allVersions
		);

		var numOfInStates = allVersions.size();
		var numOfOutStates = statesToPersist.getAllVersionedResources().size();

		System.out.println("Num of in states:  " + numOfInStates);
		System.out.println("Num of out states: " + numOfOutStates);

		statesToPersist.persist("sync-api-stat.26.05.2025.json.v2");
	}

}
