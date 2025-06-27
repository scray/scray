package org.scray.integration.ai.agent;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.networking.v1.Ingress;

import org.scray.integration.ai.agent.clients.k8s.KubernetesClient;
import org.scray.integration.ai.agent.clients.k8s.KubernetesClient.JobNotFoundException;
import org.scray.integration.ai.agent.clients.rest.RestClient;
import org.scray.integration.ai.agent.conf.Environment;
import org.scray.integration.ai.agent.conf.Environment.EnvType;
import org.scray.integration.ai.agent.conf.Environments;
import org.scray.integration.ai.agent.dto.AiJobsData;
import org.scray.integration.ai.agent.dto.JobToSchedule;
import org.scray.integration.ai.agent.dto.K8sParameters;
import org.scray.integration.ai.agent.dto.VersionedData2;
import org.slf4j.Logger;


public class AiIntegrationAgent
{

    private static final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);

    private List<String> allowedImages = new ArrayList<>();
    boolean useImageAllowList = false;

    private ObjectMapper jsonObjectMapper = new ObjectMapper();
    private RestClient apiClient = new RestClient();

    private Environments environements = null;

    private String syncApiUrl = "http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata";

    public AiIntegrationAgent(Environments envs)
    {
        this.environements = envs;
    }

    public AiIntegrationAgent(){}


    public static void main(String[] args)
        throws InterruptedException
    {

        String confLocation = System.getenv("CONFIG_PATH");

        if(confLocation != null) {
            logger.info("Env var found for configuration location {}", confLocation);
        } else {
            confLocation = Environments.defaultConfigurationLocation;
            logger.debug("Use default configuration location {}", confLocation);
        }


        Environments envs = new Environments(1, "ki1");

        try
        {
            envs.initFromFile(confLocation);

            var agent = new AiIntegrationAgent(envs);

            while (!Thread.currentThread().isInterrupted())
            {
                logger.info("Look for new jobs for env: " + envs);
                agent.pollForNewJobs();
                Thread.sleep(5000);
            }
        }
        catch (Exception e)
        {
            logger.error("Unable to load configuration from file {}", confLocation);
            e.printStackTrace();
        }

//        Environments environements = new Environments(1, "ki1", null);
//
//        var exampleEnv = new Environment(
//                                         "Environment for ki1",
//                                         "http://scray.org/ai/jobs/env/see/ki1-k8s",
//                                         EnvType.K8s,
//                                         1);
//
//
//        exampleEnv.getOrCreateWorkDir("conf");
//
//        var exampleEnv2 = new Environment(
//                                          "Environment for scray example job",
//                                          "http://scray.org/ai/jobs/env/see/stefan/python",
//                                          EnvType.K8s,
//                                          1);
//        exampleEnv2.getOrCreateWorkDir("conf");
//        exampleEnv.putEnvVar("RUNTIME_TYPE", "PYTHON22");




//        var environements = new HashMap<String, Environment>();
//        environements.put(exampleEnv.getName(), exampleEnv);
//        environements.put(exampleEnv2.getName(), exampleEnv2);

        // HashMap<String, EnvType> environements = new HashMap<String, EnvType>();
        // environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", Environment.EnvType.K8s);
        // environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", Environment.EnvType.K8s);
        // environements.put("http://scray.org/ai/jobs/env/see/ki1-standalone", Environment.EnvType.Standalone);

        // environements.put("http://scray.org/ai/jup/env/see/os/k8s", Environment.EnvType.K8s);
        // environements.put("http://scray.org/ai/app/env/see/os/k8s", Environment.EnvType.App);

        // environements.put("http://scray.org/ai/app/env/see/stefan", Environment.EnvType.K8s);
        // environements.put("http://scray.org/ai/app/env/see/stefan-t", Environment.EnvType.K8s);

        // environements.put("http://scray.org/ai/app/env/see/stefan-t", Environment.EnvType.K8s);

        // environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s/cpu/python", Environment.EnvType.Python);

        // environements.put("http://scray.org/ai/jobs/env/see/ki2-k8s/cpu/python", Environment.EnvType.Python);
        // environements.put("http://scray.org/ai/jobs/env/see/ki2-k8s/python", Environment.EnvType.Python);
        // environements.put("http://scray.org/ai/jobs/env/see/ki2-k8s", Environment.EnvType.K8s);

        // environements.put("http://scray.org/ai/jobs/env/see/os-k8s", Environment.EnvType.K8s);
        // environements.put("http://scray.org/ai/jobs/env/see/st-k8s", Environment.EnvType.K8s);


    }


    public Stream<JobToSchedule> getJobDataForThisAgent(String syncApiData, Environments myEnvs)
        throws JsonMappingException, JsonProcessingException
    {

        return Arrays.asList(jsonObjectMapper.readValue(syncApiData, VersionedData2[].class)).stream()
                     // parse job data
                     .map(versonData ->
                     {
                         try
                         {
                             return Optional.of(
                                                new JobToSchedule(versonData,
                                                                  jsonObjectMapper.readValue(versonData.getData(), AiJobsData.class)));
                         }
                         catch (JacksonException e)
                         {
                             logger.warn("No Ai job data parsed");
                             logger.debug(versonData.getData());
                             Optional<JobToSchedule> emptyJobData = Optional.empty();
                             return emptyJobData;
                         }
                     })
                     .flatMap(Optional::stream)
                     // check environment
                     .filter(jobData ->
                     {
                         if (jobData.getAiJobsData().getProcessingEnv() != null &&
                                         myEnvs.getEnvironment(jobData.getAiJobsData().getProcessingEnv()) != null)
                         {
                             var myEnv = myEnvs.getEnvironment(jobData.getAiJobsData().getProcessingEnv());
                             jobData.setK8sParameters(new K8sParameters(myEnv.getK8sJobDescriptonTemplateFullPath()));
                             return true;
                         }
                         else
                         {
                             return false;
                         }
                     });
    }


    public void setState(VersionedData2 versionedData, AiJobsData jobData, String state)
    {

        // Set new state scheduled
        jobData.setState(state);
        try
        {
            versionedData.setData(jsonObjectMapper.writeValueAsString(jobData));
            String sheduleState = jsonObjectMapper.writeValueAsString(versionedData);

            logger.info("Write scheduled state to API {}", sheduleState);
            apiClient.putData(sheduleState);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }


    public void scheduleInKubernetes(VersionedData2 versionedData, AiJobsData jobState, Environment env)
    {

        logger.info("Schedule container for {}", versionedData.getDataSource());

        if (!useImageAllowList || allowedImages.contains(jobState.getImageName()))
        {
            KubernetesClient k8sClient = new KubernetesClient();
            k8sClient.deployJob(
                                versionedData.getDataSource(),
                                env.getEnvVars().get("RUNTIME_TYPE"),
                                jobState.getImageName(), env.getK8sJobDescriptonTemplateFullPath(),
                                env.getEnvVars().get("SCRAY_SYNC_API_URL"),
                                env.getEnvVars().get("SCRAY_DATA_INTEGRATION_HOST")
                                );
        }
        else
        {
            logger.warn("Requested container not in allow list {}", jobState.getImageName());
        }
    }


    public void pollForNewJobs()
    {

        String syncApiData;
        try
        {
            syncApiData = apiClient.getData();

            this.getJobDataForThisAgent(syncApiData, environements)
                .filter(jobData -> jobData.getAiJobsData().getState().equals("UPLOADED"))
                .map(jobToStart ->
                {
                    var env = environements.getEnvironment(jobToStart.getAiJobsData().getProcessingEnv());
                    var envType = env.getType();

                    if (envType == envType.Standalone)
                    {
                        this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
                    }
                    else if (envType == envType.K8s)
                {
                    this.scheduleInKubernetes(jobToStart.getVersionData(), jobToStart.getAiJobsData(), env);
                    this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
                }
                    else if (envType == envType.Python)
                {
                    this.scheduleInKubernetes(jobToStart.getVersionData(), jobToStart.getAiJobsData(), env);
                    this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
                }
                    else if (envType == envType.App)
                {

                    logger.info("Schedule container for {}", jobToStart.getVersionData().getDataSource());

                    if (!useImageAllowList || allowedImages.contains(jobToStart.getAiJobsData().getImageName()))
                    {
                        KubernetesClient k8sClient = new KubernetesClient();
                        k8sClient.deployApp(jobToStart.getVersionData().getDataSource(),
                                            env.getEnvVars().get("RUNTIME_TYPE"),
                                            jobToStart.getAiJobsData().getImageName(),
                                            env.getK8sJobDescriptonTemplateFullPath(),
                                            env.getEnvVars().get("SCRAY_SYNC_API_URL"), // FIXME Read it fom config file
                                            env.getEnvVars().get("SCRAY_DATA_INTEGRATION_HOST") // FIXME Read it fom config file
                                            );
                    }
                    else
                    {
                        logger.warn("Requested container not in allow list {}", jobToStart.getAiJobsData().getImageName());
                    }

                    // Set state to scheduled
                    this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
                }
                    return "";
                }).toList();

            this.getJobDataForThisAgent(syncApiData, environements)
                .filter(jobData -> jobData.getAiJobsData().getState().equals("WANTED_D"))
                .map(jobToTerminate ->
                {
                    logger.info("Kill job {}", jobToTerminate.getVersionData().getDataSource());

                    try
                    {
                        KubernetesClient k8sClient = new KubernetesClient();
                        k8sClient.deleteJob(jobToTerminate.getVersionData().getDataSource());
                    }
                    catch (JobNotFoundException e)
                    {
                        logger.warn(e.getMessage());
                    }

                    this.setState(jobToTerminate.getVersionData(), jobToTerminate.getAiJobsData(), "DEAD");

                    return "";
                }).toList();

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    public void addEnv(String name, Environment type)
    {
        this.environements.addEnvironment(name, type);
    }
}
