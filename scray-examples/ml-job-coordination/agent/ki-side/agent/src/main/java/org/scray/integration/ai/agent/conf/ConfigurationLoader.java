package org.scray.integration.ai.agent.conf;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;

import org.apache.logging.log4j.message.Message;
import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class ConfigurationLoader {

	private String configurationFileLocation = System.getenv("SCRAY_K8S_AGENT_CONF");
	private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);


	public ConfigurationLoader() {
		if(configurationFileLocation == null) {
			this.configurationFileLocation = "conf/k8s-agent.conf";
		}
	}

	public ConfigurationLoader(String confLocation) {
		this.configurationFileLocation = confLocation;
	}

	public void write(Environments envs) throws ConfiguratonIoError{
		FileOutputStream outStream;
		try {
			outStream = new FileOutputStream(configurationFileLocation);
			this.write(envs, outStream);
		} catch (IOException e) {
			logger.debug(MessageFormat.format("Error while writing configuration {0}", e));
			e.printStackTrace();
			throw new ConfiguratonIoError(e.getMessage());
		}

	}

	public void write(Environments envs, OutputStream stream) throws ConfiguratonIoError, StreamWriteException, DatabindException, IOException {
		ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		objectMapper.writeValue(stream, envs);
	}

	public Environments readConfiguration() throws ConfiguratonIoError {
		ObjectMapper objectMapper = new ObjectMapper();
		Environments envs;
		try {
			var inStream = new FileInputStream(this.configurationFileLocation);
			envs = objectMapper.readValue(inStream, Environments.class);
		} catch (JsonProcessingException e) {
			logger.debug(MessageFormat.format("Error while reading configuration {0}", e));
			e.printStackTrace();
			throw new ConfiguratonIoError(e.getMessage());
		} catch (IOException e) {
			logger.debug(MessageFormat.format("Error while reading configuration {0}", e));
			e.printStackTrace();
			throw new ConfiguratonIoError(e.getMessage());
		}
		return envs;
	}

	public class ConfiguratonIoError extends Exception {
	    public ConfiguratonIoError(String errorMessage) {
	        super(errorMessage);
	    }
	}
}
