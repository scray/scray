package org.scray.sync.rest.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;

import org.scray.sync.rest.SubscriptionRegistry;
import org.scray.sync.rest.model.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/sync/subscriptions")
public class SubscriptionController {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionController.class);

    private final SubscriptionRegistry registry;

    @Value("${security.apiToken}")
    private String expectedToken;

    public SubscriptionController(SubscriptionRegistry registry) {
        this.registry = registry;
    }

    private boolean isAuthorized(HttpServletRequest request) {
        String auth = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (auth == null || !auth.startsWith("Bearer ")) return false;
        String token = auth.substring(7).trim();
        return !token.isEmpty() && token.equals(expectedToken);
    }

    @Operation(summary = "Register a subscription",
            description = "Register a callback (HTTP webhook or MQTT topic) to be notified when versioned data is updated for a given env.",
            tags = { "Sync-API-Subscriptions" })
    @io.swagger.v3.oas.annotations.parameters.RequestBody(required = true,
            content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                    schema = @Schema(implementation = Subscription.class),
                    examples = @ExampleObject(name = "HttpSubscriptionExample", value = """
                            {
                              "env":  "https://example.org/envs/production",
                              "type": "data",
                              "delivery": {
                                "type": "http",
                                "url": "https://example.org/hook",
                                "method": "POST",
                                "auth": {
                                  "type": "basic",
                                  "username": "alice",
                                  "password": "..."
                                }
                              }
                            }
                            """)))
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "Created",
                    content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE,
                            schema = @Schema(implementation = Subscription.class))) })
    @CrossOrigin(origins = "*")
    @PostMapping
    public ResponseEntity<Subscription> create(@RequestBody Subscription subscription,
                                               HttpServletRequest request) {
        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        try {
            Subscription saved = registry.register(subscription);
            logger.info("Registered subscription for env {}", saved.getEnv());
            return ResponseEntity
                    .created(URI.create("/sync/subscriptions/"))
                    .body(saved);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid subscription payload: {}", e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
        }
    }


    @Operation(summary = "Delete a subscription",
            tags = { "Sync-API-Subscriptions" })
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "Deleted"),
            @ApiResponse(responseCode = "404", description = "Not Found") })
    @CrossOrigin(origins = "*")
    @DeleteMapping("/{id}")
    public ResponseEntity<Void> delete(@PathVariable String id, HttpServletRequest request) {
        if (!isAuthorized(request)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }
        //boolean removed = registry.delete(id);
        //logger.info("Delete subscription {} -> {}", id, removed ? "removed" : "not found");
        return true //removed
                ? ResponseEntity.noContent().build()
                : ResponseEntity.status(HttpStatus.NOT_FOUND).build();
    }
}


