package org.scray.integration.ai.agent.conf;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;


public class Environments
{
    Integer version;
    String name;
    Map<String, Environment> environments;

    public static final String defaultConfigurationLocation = "conf/serialized-envs.yaml";

    private static final Logger logger = LoggerFactory.getLogger(Environments.class);

    public Environments()
    {
        super();
        this.environments = new HashMap<String, Environment>();
    }


    public Environments(Integer version, String name)
    {
        super();
        this.version = version;
        this.name = name;
        this.environments = new HashMap<String, Environment>();

    }


    public Environments(Integer version, String name, List<Environment> environments)
    {
        super();
        this.version = version;
        this.name = name;

        this.environments = this.convertToMap(environments);
    }


    public Integer getVersion()
    {
        return version;
    }


    public void setVersion(Integer version)
    {
        this.version = version;
    }


    public String getName()
    {
        return name;
    }


    public void setName(String name)
    {
        this.name = name;
    }


    public List<Environment> getEnvironments()
    {
        return this.toList(environments);
    }


    public void setEnvironments(List<Environment> environments)
    {
        this.environments = this.convertToMap(environments);
    }


    public Environment getEnvironment(String name)
    {
        return this.environments.get(name);
    }

    public void addEnvironment(String name, Environment env) {
        this.environments.put(name, env);
    }


    public List<Environment> fromYaml(String yaml)
        throws  JsonProcessingException
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        ArrayList<Environment> environements = mapper.readValue(yaml, new TypeReference<ArrayList<Environment>>(){});

       environements.stream().filter(env -> !this.isValidId(env.getId()))
       .map(env -> {
            logger.error("Invalid id {} foud. Id could contain a..z,0..9 and -.");
            return env;
        });

        return environements;
    }

    public boolean isValidId(String id) {
        // Define the regex pattern
        String regex = "^[a-z0-9-]+$";
        Pattern pattern = Pattern.compile(regex);

        return pattern.matcher(id).matches();
    }

    public String toYaml()
    {
        try
        {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER));
            return mapper.writeValueAsString(this.toList(this.environments));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            logger.error("Error while converting configuration to yaml {}", e.getMessage());
            return "";
        }
    }


    public void writeToDisk(String path)
        throws IOException
    {

        FileWriter fileWriter = null;
        try
        {
            fileWriter = new FileWriter(path);
            // Write the content to the file
            fileWriter.write(this.toYaml());
        }
        finally
        {
            if (fileWriter != null)
            {
                try
                {
                    fileWriter.close();
                }
                catch (IOException e)
                {
                    // Handle the exception (optional)
                    e.printStackTrace();
                }
            }
        }
    }


    public void initFromFile(String path)
        throws IOException
    {
        StringBuilder content = new StringBuilder();
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try
        {
            fileReader = new FileReader(path);
            bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null)
            {
                content.append(line).append("\n");
            }
        }
        finally
        {
            if (bufferedReader != null)
            {
                try
                {
                    bufferedReader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            if (fileReader != null)
            {
                try
                {
                    fileReader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
        System.out.println(content.toString());
        this.environments = this.convertToMap(this.fromYaml(content.toString()));
    }


    private Map<String, Environment> convertToMap(List<Environment> environments)
    {
        Map<String, Environment> environmentsMap = environments
                                                               .stream()
                                                               .collect(Collectors.toMap(Environment::getName, env -> env));

        return environmentsMap;
    }


    private List<Environment> toList(Map<String, Environment> envs)
    {
        return envs.values()
                   .stream()
                   .collect(Collectors.toCollection(ArrayList::new));
    }


    @Override
    public String toString()
    {
        return "Environments [version=" + version + ", name=" + name + ", environments=" + environments + "]";
    }


}
