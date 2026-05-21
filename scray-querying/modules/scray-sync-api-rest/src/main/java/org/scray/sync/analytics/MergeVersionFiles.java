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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scray.sync.api.VersionedData;
import org.scray.sync.rest.SyncFileManager;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.impl.FileVersionedDataApiImpl;


public class MergeVersionFiles
{
    private static final Logger logger = LoggerFactory.getLogger(MergeVersionFiles.class);

    public FileVersionedDataApiImpl removeState(List<String> statesToRemove, List<VersionedData> vData)
    {

        ObjectMapper objectMapper = new ObjectMapper();
        FileVersionedDataApiImpl statesToPersist = new FileVersionedDataApiImpl();

        for (VersionedData data : vData)
        {
            try
            {
                var state = objectMapper.readTree(data.getData());

                if (!statesToRemove.contains(state.get("state").asText()))
                {
                    statesToPersist.updateVersion(data);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                System.out.println("Invalid data " + data.getData());
                logger.warn("Error while parsing state attribute in version data");
            }
        }

        return statesToPersist;
    }


    public static void main(String[] args)
    {
        MergeVersionFiles merger = new MergeVersionFiles();

        List<VersionedData> f1 = new SyncFileManager("sync-api-stat-f1.json").getSyncApi().getAllVersionedResources();
        List<VersionedData> f2 = new SyncFileManager("sync-api-stat-f2.json").getSyncApi().getAllVersionedResources();

        var mergedData = merger.mergeVersionedFiles(f1, f2);
        FileVersionedDataApiImpl statesToPersist = new FileVersionedDataApiImpl();
        statesToPersist.updateVersions(mergedData);

        System.out.println("F1 size:  " + f1.size());
        System.out.println("F2 size: " + f2.size());
        System.out.println("F1 + F2 merged size: " + mergedData.size());

        statesToPersist.persist("sync-api-stat.json");

    }


    /**
     * Merge f1 and f2 to one file. In case of a conflict f1 is used.
     *
     * @param f1
     * @param f2
     */
    public List<VersionedData> mergeVersionedFiles(List<VersionedData> f1, List<VersionedData> f2)
    {

        Set<Integer> f1Keys = f1.stream()
                                .map(VersionedData::getVersionKey)
                                .collect(Collectors.toSet());

        List<VersionedData> mergedSyncDataFiles = new ArrayList<>(f1);

        // Find all which do not exist in f1
        f2.stream()
          .filter(data -> !f1Keys.contains(data.getVersionKey()))
          .forEach(mergedSyncDataFiles::add);

        return Collections.unmodifiableList(mergedSyncDataFiles);

    }


    public void removeStates()
    {
        MergeVersionFiles remover = new MergeVersionFiles();
        List<VersionedData> vsData = new SyncFileManager("sync-api.bk.stat-02.03.2026").getSyncApi().getAllVersionedResources();
        List<VersionedData> vsData2 = new SyncFileManager("sync-api-stat.bk.02.03.2026.json").getSyncApi().getAllVersionedResources();

        List<String> statesToRemove = Arrays.asList("COMPLETED", "CONVESION_ERROR", "ERROR", "PUBLISHED", "FINISHED", "LOADING_ERROR",
                                                    "UNCATEGORIZED", "CATEGORIZED");

        List<VersionedData> allVersions = Stream.concat(vsData.stream(), vsData2.stream()).toList();

        FileVersionedDataApiImpl statesToPersist = remover.removeState(
                                                                       statesToRemove,
                                                                       allVersions);

        var numOfInStates = allVersions.size();
        var numOfOutStates = statesToPersist.getAllVersionedResources().size();

        System.out.println("Num of in states:  " + numOfInStates);
        System.out.println("Num of out states: " + numOfOutStates);

        statesToPersist.persist("sync-api-stat.26.05.2025.json.v2");
    }

}
