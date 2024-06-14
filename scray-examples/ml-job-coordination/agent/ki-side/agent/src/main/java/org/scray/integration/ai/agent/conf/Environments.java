package org.scray.integration.ai.agent.conf;

import java.util.List;

import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Environments {
    public Integer version;
    public String name;
    public List<Environment> environments;

    private static final Logger logger = LoggerFactory.getLogger(Environments.class);

	public Environments() {
		super();
	}

	public Environments(Integer version, String name, List<Environment> environments) {
		super();
		this.version = version;
		this.name = name;
		this.environments = environments;
	}

	public Integer getVersion() {
		return version;
	}

	public void setVersion(Integer version) {
		this.version = version;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Environment> getEnvironments() {
		return environments;
	}

	public void setEnvironments(List<Environment> environments) {
		this.environments = environments;
	}

    public static List<Environment> fromYaml(String yaml) throws JsonMappingException, JsonProcessingException  {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, new com.fasterxml.jackson.core.type.TypeReference<List<Environment>>(){});
    }

    public String toYaml() {
        try {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.writeValueAsString(this.environments);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Error while converting configuration to yaml {}", e.getMessage() );
            return "";
        }
    }

}
