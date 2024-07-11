package org.scray.integration.ai.agent.conf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
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

    public List<Environment> fromYaml(String yaml) throws JsonMappingException, JsonProcessingException  {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(yaml, new TypeReference<List<Environment>>() {});
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

    public void writeToDisk(String path) throws IOException {

        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(path);
            // Write the content to the file
            fileWriter.write(this.toYaml());
        }
        finally {
            if (fileWriter != null) {
                try {
                    fileWriter.close();
                } catch (IOException e) {
                    // Handle the exception (optional)
                    e.printStackTrace();
                }
            }
        }
    }

    public void initFromFile(String path) throws IOException {
        StringBuilder content = new StringBuilder();
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader(path);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                content.append(line).append("\n");
            }
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fileReader != null) {
                try {
                    fileReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        this.environments = this.fromYaml(content.toString());
    }

}
