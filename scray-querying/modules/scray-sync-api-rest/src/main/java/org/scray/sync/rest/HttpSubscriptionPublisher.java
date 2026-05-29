// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
// ...
package org.scray.sync.rest;

import org.scray.sync.rest.extensions.http_publish.dto.Auth;
import org.scray.sync.rest.extensions.http_publish.dto.Delivery;
import org.scray.sync.rest.model.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import scray.sync.api.VersionedData;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class HttpSubscriptionPublisher {

    private static final Logger logger = LoggerFactory.getLogger(HttpSubscriptionPublisher.class);

    /** Max number of characters of payload/response bodies that get logged. */
    private static final int BODY_LOG_LIMIT = 2000;

    private final SubscriptionRegistry registry;
    private final HttpClient httpClient;

    public HttpSubscriptionPublisher(SubscriptionRegistry registry) {
        this.registry = registry;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    public void publish(VersionedData versionedData) {
        String env = versionedData.getEnv();
        if (env == null || env.isBlank()) {
            logger.debug("VersionedData has no env, nothing to publish");
            return;
        }

        List<Subscription> subscribers = registry.findByEnv(env);
        if (subscribers.isEmpty()) {
            logger.debug("No subscribers for env {}", env);
            return;
        }

        String payload = "{\"data\": \"" + versionedData.getData() + "\"}";
        logger.info("Publishing update for env {} to {} subscriber(s), payload size={} chars",
                env, subscribers.size(), payload == null ? 0 : payload.length());

        for (Subscription sub : subscribers) {
            Delivery delivery = sub.getDelivery();
            if (delivery == null || !"https".equalsIgnoreCase(delivery.getType()) || !"https".equalsIgnoreCase(delivery.getType())) {
                logger.debug("Skipping subscription {} — delivery type is not http", sub);
                continue;
            }
            dispatch(sub, delivery, payload);
        }
    }

    private void dispatch(Subscription sub, Delivery delivery, String payload) {
        String reqId = UUID.randomUUID().toString().substring(0, 8);
        String method = delivery.getMethod() == null ? "POST" : delivery.getMethod().toUpperCase();
        String url = delivery.getUrl();

        try {
            HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));

            applyAuth(builder, delivery.getAuth());

            HttpRequest request = builder.build();

            // --- log outgoing request -----------------------------------------
            logger.info("[req {}] -> {} {}  (payload {} chars)",
                    reqId, method, url,
                    payload == null ? 0 : payload.length());
            if (logger.isDebugEnabled()) {
                logger.debug("[req {}] headers={}", reqId, request.headers().map());
                logger.debug("[req {}] body={}", reqId, truncate(payload));
            }

            long start = System.nanoTime();
            CompletableFuture<HttpResponse<String>> future =
                    httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString());

            future.whenComplete((response, error) -> {
                long elapsedMs = (System.nanoTime() - start) / 1_000_000L;

                if (error != null) {
                    logger.warn("[req {}] FAILED after {} ms: {} ({})",
                            reqId, elapsedMs,
                            error.getClass().getSimpleName(), error.getMessage());
                    return;
                }

                int status = response.statusCode();
                String body = response.body();
                if (status >= 200 && status < 300) {
                    logger.info("[req {}] <- {} from {} in {} ms ({} chars)",
                            reqId, status, url, elapsedMs,
                            body == null ? 0 : body.length());
                } else {
                    logger.warn("[req {}] <- {} from {} in {} ms",
                            reqId, status, url, elapsedMs);
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("[req {}] response headers={}", reqId, response.headers().map());
                    logger.debug("[req {}] response body={}", reqId, truncate(body));
                }
            });
        } catch (Exception e) {
            logger.warn("[req {}] failed to build request for {}: {}",
                    reqId, url, e.getMessage(), e);
        }
    }

    private void applyAuth(HttpRequest.Builder builder, Auth auth) {
        if (auth == null || auth.getType() == null || "none".equalsIgnoreCase(auth.getType())) {
            logger.debug("No auth applied");
            return;
        }
        switch (auth.getType().toLowerCase()) {
            case "basic" -> {
                String credentials = auth.getUsername() + ":" + auth.getPassword();
                String encoded = Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
                builder.header("Authorization", "Basic " + encoded);
                logger.debug("Applied Basic auth for user {}", auth.getUsername());
            }
            case "bearer" -> {
                builder.header("Authorization", "Bearer " + auth.getPassword());
                logger.debug("Applied Bearer auth");
            }
            default -> logger.warn("Unsupported auth type: {}", auth.getType());
        }
    }

    private static String truncate(String s) {
        if (s == null) return "<null>";
        if (s.length() <= BODY_LOG_LIMIT) return s;
        return s.substring(0, BODY_LOG_LIMIT) + "... [truncated, " + s.length() + " chars total]";
    }
}