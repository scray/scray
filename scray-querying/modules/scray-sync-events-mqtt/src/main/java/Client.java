import java.lang.StackWalker.Option;
import java.util.Optional;
import java.util.UUID;

import org.eclipse.paho.client.mqttv3.IMqttClient;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.scray.sync.out.mqtt.MqttChannel;

public class Client {

	String mqttHost = null;
	IMqttClient publisher = null;

	public void send() {
		String publisherId = UUID.randomUUID().toString();
		var publisher = new MqttChannel(
				"tcp://ml-integration.research.dev.seeburger.de:1883",
				Optional.empty(),
				Optional.empty());

		publisher.publish("/topic1/", "message_" + System.currentTimeMillis());
	}

}
