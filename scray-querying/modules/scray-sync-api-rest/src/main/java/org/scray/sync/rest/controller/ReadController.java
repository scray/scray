package org.scray.sync.rest.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.scray.sync.rest.SyncFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import scala.Option;
import scray.sync.api.VersionedData;
import scray.sync.impl.FileVersionedDataApiImpl;

import java.io.FileNotFoundException;
import java.util.List;

@RestController
public class ReadController {

    private static final Logger logger = LoggerFactory.getLogger(ReadController.class);
    SyncFileManager syncApiManager = new SyncFileManager("sync-api-stat.json");

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
    @CrossOrigin(origins = "*")
    @GetMapping(value = "/sync/versioneddata/latest")
    ResponseEntity<VersionedData> getLatestVersion(@RequestParam String datasource, @RequestParam String mergekey) {

        if(datasource == null && mergekey == null) {
            syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);
        }

        Option<VersionedData> latestVersion = syncApiManager.getSyncApi().getLatestVersion(datasource, mergekey);

        if(latestVersion.isEmpty()) {
            return new ResponseEntity<VersionedData>(HttpStatus.NOT_FOUND);
        } else {
            return new  ResponseEntity<VersionedData>(latestVersion.get(), HttpStatus.OK);
        }
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
    }

}
