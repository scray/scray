package org.scray.sync.out.mqtt;

import java.util.Optional;
import java.util.UUID;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MqttChannel {

	IMqttClient publisher = null;
	String host;
	Optional<String> user;
	Optional<String> password;
	int qos = 0;

	private static final Logger logger = LoggerFactory.getLogger(MqttChannel.class);

	public MqttChannel(String host, Optional<String> user, Optional<String> password) {
		super();
		this.host = host;
		this.user = user;
		this.password = password;
	}

	private void initPublisher(String mqttHost, Optional<String> user, Optional<String> password) {
		String publisherId = UUID.randomUUID().toString();
		try {
			publisher = new MqttClient(mqttHost, publisherId, new MemoryPersistence());

			MqttConnectOptions options = new MqttConnectOptions();
			// options.setAutomaticReconnect(true);
			// options.setCleanSession(true);
			options.setConnectionTimeout(10);
			options.setKeepAliveInterval(10);

			user.ifPresent(options::setUserName);
			password.ifPresent(passwordStr -> options.setPassword(passwordStr.toCharArray()));

			logger.debug("Connect publisher");
			publisher.connect(options);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void publish(String topic, String messageString) {
		if (publisher == null) {
			this.initPublisher(host, user, password);
		}

		MqttMessage message = new MqttMessage(messageString.getBytes());
		message.setQos(qos);
		try {
			publisher.publish(topic, message);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	public void disconnect() {
		try {
			publisher.disconnect();
			publisher.close();
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

}
