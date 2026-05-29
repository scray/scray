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
import io.swagger.v3.oas.annotations.media.ExampleObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import scala.Option;

import org.scray.sync.rest.FilterParser;
import org.scray.sync.rest.HttpSubscriptionPublisher;
import org.scray.sync.rest.PersistBuffer;
import org.scray.sync.rest.SearchRequest;
import org.scray.sync.rest.SyncEventManager;
import org.scray.sync.rest.SyncFileManager;
import org.scray.sync.rest.extensions.mqtt_publish.MqttSyncEventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.QuerySpec;
import scray.sync.api.QuerySpec.Condition;
import scray.sync.api.VersionedData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

@RestController
@SpringBootApplication(
		exclude = {org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration.class }
	)
public class ReadController {

	private static final Logger logger = LoggerFactory.getLogger(ReadController.class);
	SyncEventManager eventManager = new MqttSyncEventManager();
    private final HttpSubscriptionPublisher httpPublisher;
	private final PersistBuffer persistBuffer;
	SyncFileManager syncApiManager;

    // Inject your expected token from application.properties or env
    @Value("${security.apiToken}")
    private String expectedToken;


    private boolean isAuthorized(HttpServletRequest request) {
        String auth = request.getHeader(HttpHeaders.AUTHORIZATION); // "Authorization"
        if (auth == null || !auth.startsWith("Bearer ")) return false;
        String token = auth.substring(7).trim();
        return !token.isEmpty() && token.equals(expectedToken);
    }


    public ReadController(PersistBuffer buffer, HttpSubscriptionPublisher httpPublisher) {
        this.persistBuffer = buffer;
        this.syncApiManager = buffer.getSyncApiManager();
        this.httpPublisher = httpPublisher;
    }
	@Operation(summary = "Get latest version", description = "Get latest version of the data",

			tags = { "Sync-API" })
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = VersionedData.class))) })
	@Parameter(name = "filter", description = "Optional filters on data JSON fields, e.g., data.env==http%3A%2F%2Fexample-env.scray.org;data.job.meta.name==job1")
	@CrossOrigin(origins = "*")
	@GetMapping(value = "/sync/versioneddata/latest")
	ResponseEntity<VersionedData> getLatestVersion(@RequestParam String datasource, @RequestParam String mergekey,
			@RequestParam(required = false) String filter, HttpServletRequest request) {

        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

		if (datasource == null && mergekey == null) {
			syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);
		}

		Optional<VersionedData> latestVersion = syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);

		if (latestVersion.isEmpty()) {
			return new ResponseEntity<VersionedData>(HttpStatus.NOT_FOUND);
		} else {
			return new ResponseEntity<VersionedData>(latestVersion.get(), HttpStatus.OK);
		}

	}

	private final FilterParser parser = new FilterParser();

	@io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, examples = @ExampleObject(name = "SearchRequestExample", value = "{\n"
			+ "  \"filter\": \"data.processingEnv==http://scray.org/ai/app/env/see/os/k8s;data.state==RUNNING\"\n"
			+ "}")))
	@PostMapping("/indexes/{indexName}/search")
	public ResponseEntity<List<VersionedData>> search(@PathVariable String indexName,
			@RequestBody SearchRequest searchRequest, HttpServletRequest request) {

        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }


		QuerySpec filter = parser.parse(searchRequest.filter());
		if (filter != null && filter.conditions().size() == 2) {
			try {

				String processingEnv = filter.conditions().get("data.processingEnv").getValue();
				String state = filter.conditions().get("data.state").getValue();

				Optional<List<VersionedData>> latestVersion = syncApiManager.getSyncApi().getLatestVersion("EnvState",
						processingEnv, state);
				if (latestVersion.isEmpty()) {
					return new ResponseEntity("No entry found for given filter", HttpStatus.NOT_FOUND);
				} else {
					return new ResponseEntity(latestVersion, HttpStatus.OK);
				}
			} catch (Exception e) {
				e.printStackTrace();
				return new ResponseEntity<>(HttpStatus.BAD_REQUEST); // bad JSON or bad filter
			}
		} else {
			return new ResponseEntity(HttpStatus.NOT_FOUND);
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
			if (current == null)
				return null;
			current = current.get(key);
		}
		return current;
	}

	@Operation(summary = "Get lates versions of all versioned resources.", description = "A list with all versioned resource of this user",

			tags = { "Sync-API" })
	@ApiResponses(value = {
			@ApiResponse(responseCode = "200", description = "OK", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = VersionedData.class))) })
	@CrossOrigin(origins = "*")
	@GetMapping(value = "/sync/versioneddata/all/latest")
	public ResponseEntity<List<VersionedData>> getLatestVersion(HttpServletRequest request) {

        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

		return new ResponseEntity<>(syncApiManager.getSyncApi().getAllVersionedResources(), HttpStatus.OK);
	}

	@Operation(summary = "Update Version", description = "Update a version",

			tags = { "Sync-API" })
	@io.swagger.v3.oas.annotations.parameters.RequestBody(required = true, content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE, schema = @Schema(implementation = VersionedData.class), examples = @ExampleObject(name = "VersionedDataExample", value = "{\n"
			+ "  \"dataSource\": \"s1\",\n" + "  \"mergeKey\": \"_\",\n" + "  \"version\": 0,\n"
			+ "  \"data\": \"{\\\"processingEnv\\\": \\\"http://scray.org/ai/app/env/see/os/k8s\\\",  \\\"state\\\": \\\"RUNNING\\\"}\",\n"
			+ "  \"versionKey\": 0\n" + "}")))
	@ApiResponses(value = { @ApiResponse(responseCode = "200", description = "OK") })
	@CrossOrigin(origins = "*")
	@PutMapping(value = "/sync/versioneddata/latest")
    ResponseEntity<?> updateVersion(@RequestBody VersionedData updatedVersionedData,
                                     HttpServletRequest request) {
        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // assign a fresh dataSource id if the caller didn't provide one
        if (updatedVersionedData.getDataSource() == null
                || updatedVersionedData.getDataSource().isBlank()) {
            updatedVersionedData.setDataSource(UUID.randomUUID() + "_" + System.currentTimeMillis());
        }

        syncApiManager.getSyncApi().updateVersion(updatedVersionedData);
        persistBuffer.markDirty();
        try {
            eventManager.publishUpdate(updatedVersionedData);
            httpPublisher.publish(updatedVersionedData);
        } catch (Exception e) {
            logger.warn("Error when sending event notification {} ", e);
        }
        return ResponseEntity.status(HttpStatus.OK).build();
    }
}
