package org.scray.sync.rest;

import java.util.Optional;

import org.scray.sync.out.mqtt.MqttChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.VersionedData;
import scray.sync.api.VersionedDataApi;

public class MqttSyncEventManager implements SyncEventManager {
	private static final Logger logger = LoggerFactory.getLogger(SyncFileManager.class);

	String host = System.getenv("SCRAY_EVENT_MQTT_HOST");
	String topic = System.getenv("SCRAY_EVENT_MQTT_TOPIC");
	String user = System.getenv("SCRAY_EVENT_MQTT_USER");
	String pw = System.getenv("SCRAY_EVENT_MQTT_PW");

	MqttChannel publisher = null;

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

			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}
	}

}
