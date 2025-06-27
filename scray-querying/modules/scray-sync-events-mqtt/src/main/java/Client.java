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

	public static void main(String[] args) throws InterruptedException {
		send();
	}

	public static void send() throws InterruptedException {

		String message = "\r\n"
				+ "  {\r\n"
				+ "    \"dataSource\": \"tutor\",\r\n"
				+ "    \"mergeKey\": \"_\",\r\n"
				+ "    \"version\": 0,\r\n"
				+ "    \"data\": \"{\\\"filename\\\": \\\"tutor.tar.gz\\\", \\\"state\\\": \\\"COMPLETED\\\",  \\\"dataDir\\\": \\\"./\\\", \\\"notebookName\\\": \\\"app_frontend.py\\\"}\",\r\n"
				+ "    \"versionKey\": -862364917\r\n"
				+ "  },";

		String publisherId = UUID.randomUUID().toString();
		var publisher = new MqttChannel(
				"tcp://ml-integration.research.dev.example.com:1883",
				Optional.empty(),
				Optional.empty());

		for (int i = 0; i < 100; i++) {
			publisher.publish("/topic1/", message);

			Thread.sleep(4000);
		}



	}

}
