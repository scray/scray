/*
 * EnvironementsTests.java
 *
 * created at 2024-06-12 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.scray.integration.ai.agent.conf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class EnvironementsTests
{
    @Test
    public void yamlSerialisationTest() {

        var env1 = new Environment(
                                  "Test env 1",
                                  "http://scray.org/test/env",
                                  Environment.EnvType.K8s,
                                  1);
        var env2 = new Environment(
                                  "Test env 2",
                                  "http://scray.org/test/env2",
                                  Environment.EnvType.K8s,
                                  1);

        List<Environment> envList = new ArrayList<Environment>();

        envList.add(env1);
        envList.add(env2);
        env2.getK8sJobDescriptonTemplate();

        Environments envs = new Environments(1, "ki1", envList);

        // Serialize configurations
        String envYaml = envs.toYaml();
        System.out.println(envYaml);
        Assertions.assertTrue(envYaml.contains("k8sJobDescriptonTemplate: \"job2.yaml\""));

        // Deserialize configuration.

        Environments envs2 = new Environments(1, "ki1");
        List<Environment> loadedEnvs;
        try
        {
            loadedEnvs = envs2.fromYaml(envYaml);
            Assertions.assertEquals(2, loadedEnvs.size());
        }
        catch (Exception e)
        {
            Assertions.fail("Error while pasing yaml {}", e);
            e.printStackTrace();
        }

    }

    @Test
    public void initFromFile() {
        var env1 = new Environment(
                                   "Test env 1",
                                   "http://scray.org/test/env1",
                                   Environment.EnvType.K8s,
                                   1);
        env1.setIngressTemplate("fff");
        env1.setK8sJobDescriptonTemplate("job42.yaml");

        env1.putEnvVar("JAVA_HOME", "/opt/java/spdf-21/");

         var env2 = new Environment(
                                   "Test env 2",
                                   "http://scray.org/test/env2",
                                   Environment.EnvType.Python,
                                   2);
         env2.setIngressTemplate("fffs");
         env2.putEnvVar("JAVA_HOME", "/opt/java/spdf-17/");
         env2.putEnvVar("SCRAY_SYNC_API_URL", "ml-integration.research.dev.example.com:8082");
         env2.putEnvVar("SCRAY_DATA_INTEGRATION_HOST", "ml-integration.research.dev.example.com");

         List<Environment> envList = new ArrayList<Environment>();

         envList.add(env1);
         envList.add(env2);
         env2.getK8sJobDescriptonTemplate();
         System.out.println(env2.getK8sJobDescriptonTemplate());

         Environments envs = new Environments(1, "ki1", envList);

         try
        {
            envs.writeToDisk("target/serialized-envs.yaml");

            Environments loadedEnvs = new Environments();
            loadedEnvs.initFromFile("target/serialized-envs.yaml");

            Assertions.assertEquals(2, loadedEnvs.getEnvironments().size());
            System.out.println(loadedEnvs.getEnvironments().get(0).getId());
            Assertions.assertEquals("http://scray.org/test/env1", loadedEnvs.getEnvironments().get(0).getName());
            Assertions.assertEquals(Environment.EnvType.K8s, loadedEnvs.getEnvironments().get(0).getType());
            Assertions.assertEquals(1, loadedEnvs.getEnvironments().get(0).getVersion());
            Assertions.assertEquals("job42.yaml", loadedEnvs.getEnvironments().get(0).getK8sJobDescriptonTemplate());



            Assertions.assertEquals("http://scray.org/test/env2", loadedEnvs.getEnvironments().get(1).getName());
            Assertions.assertEquals(Environment.EnvType.Python, loadedEnvs.getEnvironments().get(1).getType());
            Assertions.assertEquals(2, loadedEnvs.getEnvironments().get(1).getVersion());
            Assertions.assertEquals("ml-integration.research.dev.example.com:8082", loadedEnvs.getEnvironments().get(1).getEnvVars().get("SCRAY_SYNC_API_URL"));




        }
        catch (IOException e)
        {
            e.printStackTrace();

            Assertions.fail("IO Exception " + e.getMessage() );
        }

    }

}



