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

package org.scray.sync.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.scray.sync.rest.controller.SubscriptionController;
import org.scray.sync.rest.model.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Component
public class VersionedDataSubscriptionRegistry implements SubscriptionRegistry {

    private static final Logger logger = LoggerFactory.getLogger(VersionedDataSubscriptionRegistry.class);


    private final ObjectMapper mapper = new ObjectMapper();
    private List<Subscription> subscriptions = new ArrayList<>();


    @Override
    public Subscription register(Subscription subscription) {
        if (subscription.getEnv() == null || subscription.getEnv().isBlank()) {
            throw new IllegalArgumentException("env is required");
        }
        if (subscription.getDelivery() == null) {
            throw new IllegalArgumentException("delivery is required");
        }

        subscriptions.add(subscription);

        logger.debug("Add new subscription {}", subscription);
        logger.debug("Number of subscriptions {}", subscriptions.size());
        return subscription;
    }


    @Override
    public List<Subscription> findByEnv(String envIri) {
        return subscriptions.stream()
                .filter(s -> envIri.equals(s.getEnv()))
                .toList();
    }


    private String serialize(Subscription sub) {
        try {
            return mapper.writeValueAsString(sub);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize subscription ", e);
        }
    }

    private Subscription deserialize(VersionedData vd) {
        try {
            return mapper.readValue(vd.getData(), Subscription.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to deserialize subscription " + vd, e);
        }
    }
}