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

package org.scray.sync.rest.extensions.mqtt_publish;

import java.util.Optional;

import org.scray.sync.out.mqtt.MqttChannel;
import org.scray.sync.rest.PublishedEventStatistics;
import org.scray.sync.rest.SyncEventManager;
import org.scray.sync.rest.SyncFileManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.VersionedData;

public class MqttSyncEventManager implements SyncEventManager {
	private static final Logger logger = LoggerFactory.getLogger(SyncFileManager.class);

	String host = System.getenv("SCRAY_EVENT_MQTT_HOST");
	String topic = System.getenv("SCRAY_EVENT_MQTT_TOPIC");
	String user = System.getenv("SCRAY_EVENT_MQTT_USER");
	String pw = System.getenv("SCRAY_EVENT_MQTT_PW");

	MqttChannel publisher = null;

	PublishedEventStatistics statisticsUnit = new PublishedEventStatistics();

	private void initClient() {
		if (host == null) {
			logger.warn("No MQTT host defined. Not event will be sent. ");
		} else {
			this.publisher = new MqttChannel(host, Optional.ofNullable(user), Optional.ofNullable(pw));
		}
	}

	@Override
	public void publishUpdate(VersionedData updatedVersionedData) {

		if (host != null) {
			if (publisher == null) {
				this.initClient();
			}

			ObjectMapper objectMapper = new ObjectMapper();
			String versionedDataString;
			try {
				versionedDataString = objectMapper.writeValueAsString(updatedVersionedData);

				logger.debug("Public scray sync update event");
				this.publisher.publish(topic, versionedDataString);

				this.statisticsUnit.setPublishedEvents(statisticsUnit.getPublishedEvents() + 1);
				this.statisticsUnit.setPubshingNotAcknowledged(this.publisher.getPendingDeliveryTokensSize());

				logger.debug("Publisher statistics {}:", statisticsUnit);
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public PublishedEventStatistics getStatistics() {
		return this.statisticsUnit;
	}

}
