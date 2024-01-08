package org.scray.integration.ai.agent.conf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.scray.integration.ai.agent.conf.ConfigurationLoader.ConfiguratonIoError;
import org.scray.integration.ai.agent.conf.Environment.EnvType;

public class ConfigurationLoaderTests {

	@Test
	public void writeTest() {
		var confLoader = new ConfigurationLoader("./target/writeTest.json");

		var env = new Environment("App k8s env", "http://scray.org/ai/app/env/see/ki1-k8s", EnvType.K8s);
		var envs = new Environments("", "", Arrays.asList(env));

		try {
			confLoader.write(envs);

			var confLoaderRead = new ConfigurationLoader("./target/writeTest.json");
			var config = confLoaderRead.readConfiguration();
			assertEquals("http://scray.org/ai/app/env/see/ki1-k8s", config.getEnvironments().get(0).getId());
			assertEquals(EnvType.K8s, config.getEnvironments().get(0).getType());

		} catch (ConfiguratonIoError e) {
			fail(e);
		}
	}


}
