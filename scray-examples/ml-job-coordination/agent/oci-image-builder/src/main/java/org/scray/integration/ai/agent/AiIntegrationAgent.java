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

import org.scray.integration.ai.agent.clients.rest.RestClient;
import org.scray.integration.ai.agent.dto.AiJobsData;
import org.scray.integration.ai.agent.dto.Environment;
import org.scray.integration.ai.agent.dto.Environment.EnvType;
import org.scray.integration.oci.ImageBuilder;
import org.scray.integration.ai.agent.dto.JobToSchedule;
import org.scray.integration.ai.agent.dto.K8sParameters;
import org.scray.integration.ai.agent.dto.VersionedData2;
import org.slf4j.Logger;

public class AiIntegrationAgent {

	private static final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);

	private List<String> allowedImages = new ArrayList<>();
	boolean useImageAllowList = false;
	private HashMap<String, EnvType> environements = null;

	private ObjectMapper jsonObjectMapper = new ObjectMapper();
	private RestClient apiClient = new RestClient();

	private String syncApiUrl = "http://ml-integration.research.dev.seeburger.de:8082";

	public AiIntegrationAgent() {}

	public AiIntegrationAgent(HashMap<String, EnvType> environements) {
		this.environements = environements;
	}

	public static void main(String[] args) throws InterruptedException {


		HashMap<String, EnvType> environements = new HashMap<String, EnvType>();
		//environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", 		 Environment.EnvType.K8s);
		//environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", 		 Environment.EnvType.K8s);
		//environements.put("http://scray.org/ai/jobs/env/see/ki1-standalone", Environment.EnvType.Standalone);

		//environements.put("http://scray.org/ai/jup/env/see/os/k8s", Environment.EnvType.K8s);
		//environements.put("http://scray.org/ai/app/env/see/os/k8s", Environment.EnvType.App);

		//environements.put("http://scray.org/ai/app/env/see/stefan", Environment.EnvType.K8s);

		environements.put("http://research.dev.seeburger.de/oci/image/", Environment.EnvType.OciBuild);

		//environements.put("http://scray.org/ai/app/env/see/stefan-t", Environment.EnvType.K8s);




		//environements.put("http://scray.org/ai/jobs/env/see/os-k8s", 		 Environment.EnvType.K8s);
		//environements.put("http://scray.org/ai/jobs/env/see/st-k8s", 		 Environment.EnvType.K8s);

		var agent = new AiIntegrationAgent(environements);

		while (!Thread.currentThread().isInterrupted()) {
			logger.info("Look for new jobs for env: " + environements);
			agent.pollForNewJobs();
			Thread.sleep(5000);
		}
	}

	public Stream<JobToSchedule> getJobDataForThisAgent(String syncApiData, Set<String> myEnvs)
			throws JsonMappingException, JsonProcessingException {

		return Arrays.asList(jsonObjectMapper.readValue(syncApiData, VersionedData2[].class)).stream()
				// parse job data
				.map(versonData -> {
					try {
						return Optional.of(
								new JobToSchedule(versonData,
										jsonObjectMapper.readValue(versonData.getData(), AiJobsData.class), new K8sParameters("job2.yaml")) // FIXME K8sParameter should be a parameter
								);
					} catch (JacksonException e) {
						logger.warn("No Ai job data parsed");
						logger.debug(versonData.getData());
						Optional<JobToSchedule> emptyJobData = Optional.empty();
						return emptyJobData;
					}
				})
				.flatMap(Optional::stream)
				// check environment
				.filter(jobData -> {
					if (jobData.getAiJobsData().getProcessingEnv() != null && myEnvs.contains(jobData.getAiJobsData().getProcessingEnv())) {
						return true;
					} else {
						return false;
					}
				})
				;
	}


	public void setState(VersionedData2 versionedData, AiJobsData jobData, String state) {

		// Set new state scheduled
		jobData.setState(state);
		try {
			versionedData.setData(jsonObjectMapper.writeValueAsString(jobData));
			String sheduleState = jsonObjectMapper.writeValueAsString(versionedData);

			logger.info("Write scheduled state to API {}", sheduleState);
			apiClient.putData(sheduleState);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}



	public void pollForNewJobs() {

			String syncApiData;
			try {
				syncApiData = apiClient.getData();

				this.getJobDataForThisAgent(syncApiData, environements.keySet())
				.filter(jobData -> jobData.getAiJobsData().getState().equals("UPLOADED"))
				.map(jobToStart -> {
					var envType = environements.get(jobToStart.getAiJobsData().getProcessingEnv());

					if(envType == envType.OciBuild) {
					    this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "RUNNING");

					    if (jobToStart.getAiJobsData().getImageName() != null) {

    				        var builder = new ImageBuilder();
    				        builder.run("registry.research.dev.seeburger.de:5000",
    				                    jobToStart.getAiJobsData().getImageName(),
    				                    jobToStart.getAiJobsData().getFilename());
					    } else {
					        this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "IMAGE_NAME_MISSING");

					    }

					    this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "COMPLETED");
					}
					return "";
				}).toList();

				this.getJobDataForThisAgent(syncApiData, environements.keySet())
				.filter(jobData -> jobData.getAiJobsData().getState().equals("WANTED_D"))
				.map(jobToTerminate -> {
					logger.info("Kill job {}", jobToTerminate.getVersionData().getDataSource());


					this.setState(jobToTerminate.getVersionData(), jobToTerminate.getAiJobsData(), "DEAD");

					return "";
				}).toList();


			} catch (Exception e) {
				e.printStackTrace();
			}
	}

	public void addEnv(String name, EnvType type) {
		this.environements.put(name, type);
	}
}