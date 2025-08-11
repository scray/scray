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

package org.scray.sync.rest.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import scala.Option;

import org.scray.sync.rest.MqttSyncEventManager;
import org.scray.sync.rest.SyncEventManager;
import org.scray.sync.rest.SyncFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.VersionedData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
public class ReadController {

    private static final Logger logger = LoggerFactory.getLogger(ReadController.class);
    SyncFileManager syncApiManager = new SyncFileManager("sync-api-stat.json");
    SyncEventManager eventManager = new MqttSyncEventManager();


    @Operation(summary = "Get latest version",
            description = "Get latest version of the data",

            tags = { "Sync-API" })
    @ApiResponses(value =
            {
                    @ApiResponse(responseCode = "200",
                            description = "OK",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    schema = @Schema(implementation = VersionedData.class))) })
    @Parameter(name = "filter", description = "Optional filters on data JSON fields, e.g., data.env==http%3A%2F%2Fexample-env.scray.org;data.job.meta.name==job1")
    @CrossOrigin(origins = "*")
    @GetMapping(value = "/sync/versioneddata/latest")
    ResponseEntity<VersionedData> getLatestVersion(
    		@RequestParam String datasource,
    		@RequestParam String mergekey,
    		@RequestParam(required = false) String filter) {

        if(datasource == null && mergekey == null) {
            syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);
        }


        Optional<VersionedData> latestVersion = syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);

        if(latestVersion.isEmpty()) {
            return new ResponseEntity<VersionedData>(HttpStatus.NOT_FOUND);
        } else {

        	var latestVersionData = latestVersion.get();

            if (filter != null && !filter.isBlank()) {
                try {
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode dataJson = objectMapper.readTree(latestVersionData.getData());
                    Map<String, String> filters = parseFilterString(filter);

                    for (Map.Entry<String, String> entry : filters.entrySet()) {
                        String jsonPath = entry.getKey();
                        String expectedValue = entry.getValue();

                        JsonNode actualNode = getNestedJsonNode(dataJson, jsonPath);
                        if (actualNode == null || !expectedValue.equals(actualNode.asText())) {
                            return new ResponseEntity<>(HttpStatus.NOT_FOUND); // does not match filter
                        }
                    }

                } catch (Exception e) {
                    return new ResponseEntity<>(HttpStatus.BAD_REQUEST); // bad JSON or bad filter
                }
            }

            return new  ResponseEntity<VersionedData>(latestVersion.get(), HttpStatus.OK);
        }
    }


    private Map<String, String> parseFilterString(String filter) {
        Map<String, String> result = new HashMap<>();
        String[] filters = filter.split(";");
        for (String clause : filters) {
            String[] parts = clause.split("==", 2);
            if (parts.length == 2) {
                result.put(parts[0].trim(), parts[1].trim());
            } else {
                throw new IllegalArgumentException("Invalid filter format: " + clause);
            }
        }
        return result;
    }

    private JsonNode getNestedJsonNode(JsonNode root, String path) {
        String[] keys = path.split("\\.");
        JsonNode current = root;
        for (String key : keys) {
            if (current == null) return null;
            current = current.get(key);
        }
        return current;
    }



    @Operation(summary = "Get lates versions of all versioned resources.",
            description = "A list with all versioned resource of this user",

            tags = { "Sync-API" })
    @ApiResponses(value =
            {
                    @ApiResponse(responseCode = "200",
                            description = "OK",
                            content = @Content(
                                    mediaType = MediaType.APPLICATION_JSON_VALUE,
                                    schema = @Schema(implementation = VersionedData.class))) })
    @CrossOrigin(origins = "*")
    @GetMapping(value = "/sync/versioneddata/all/latest")
    public ResponseEntity<List<VersionedData>> getLatestVersion() {
    	return new ResponseEntity<>(syncApiManager.getSyncApi().getAllVersionedResources(), HttpStatus.OK);
    }



    @Operation(summary = "Update Version",
            description = "Update a version",

            tags = { "Sync-API" })
    @ApiResponses(value =
            {
                    @ApiResponse(responseCode = "200",
                            description = "OK")
            })
    @CrossOrigin(origins = "*")
    @PutMapping(value = "/sync/versioneddata/latest")
    void updateVersion(@RequestBody VersionedData updatedVersionedData) {
        syncApiManager.getSyncApi().updateVersion(updatedVersionedData);
        syncApiManager.persist();
        try {
        eventManager.publishUpdate(updatedVersionedData);
        } catch (Exception e) {
        	logger.warn("Error when sending event notification {} ", e);
        }
    }
}
