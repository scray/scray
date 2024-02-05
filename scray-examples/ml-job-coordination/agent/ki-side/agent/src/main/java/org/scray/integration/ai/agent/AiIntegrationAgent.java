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
import org.scray.integration.ai.agent.dto.AiJobsData;
import org.scray.integration.ai.agent.dto.Environment;
import org.scray.integration.ai.agent.dto.Environment.EnvType;
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

	public AiIntegrationAgent() {}

	public AiIntegrationAgent(HashMap<String, EnvType> environements) {
		this.environements = environements;
	}

	public static void main(String[] args) throws InterruptedException {

		HashMap<String, EnvType> environements = new HashMap<String, EnvType>();
		environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", 		 Environment.EnvType.K8s);
		environements.put("http://scray.org/ai/jobs/env/see/ki1-k8s", 		 Environment.EnvType.K8s);
		environements.put("http://scray.org/ai/jobs/env/see/ki1-standalone", Environment.EnvType.Standalone);

		//environements.put("http://scray.org/ai/jup/env/see/os/k8s", Environment.EnvType.K8s);
		//environements.put("http://scray.org/ai/app/env/see/os/k8s", Environment.EnvType.App);

		//environements.put("http://scray.org/ai/app/env/see/stefan", Environment.EnvType.K8s);
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

	public void scheduleInKubernetes(VersionedData2 versionedData, AiJobsData jobState) {

		logger.info("Schedule container for {}", versionedData.getDataSource());

		if(!useImageAllowList || allowedImages.contains(jobState.getImageName())) {
			KubernetesClient k8sClient = new KubernetesClient();
			k8sClient.deployJob(versionedData.getDataSource(), jobState.getImageName(), jobState.getJobTemplateFile());
		} else {
			logger.warn("Requested container not in allow list {}", jobState.getImageName());
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

					if(envType == envType.Standalone) {
						this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
					} else if(envType == envType.K8s) {
						this.scheduleInKubernetes(jobToStart.getVersionData(), jobToStart.getAiJobsData());
						this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
					} else if(envType == envType.App) {


						logger.info("Schedule container for {}", jobToStart.getVersionData().getDataSource());

						if(!useImageAllowList || allowedImages.contains(jobToStart.getAiJobsData().getImageName())) {
							KubernetesClient k8sClient = new KubernetesClient();
						    jobToStart.getAiJobsData().setJobTemplateFile("app-job.yaml");
							k8sClient.deployApp(jobToStart.getVersionData().getDataSource(), jobToStart.getAiJobsData().getImageName(), jobToStart.getAiJobsData().getJobTemplateFile());
						} else {
							logger.warn("Requested container not in allow list {}", jobToStart.getAiJobsData().getImageName());
						}


						// Set state to scheduled
						this.setState(jobToStart.getVersionData(), jobToStart.getAiJobsData(), "SCHEDULED");
					}
					return "";
				}).toList();

				this.getJobDataForThisAgent(syncApiData, environements.keySet())
				.filter(jobData -> jobData.getAiJobsData().getState().equals("WANTED_D"))
				.map(jobToTerminate -> {
					logger.info("Kill job {}", jobToTerminate.getVersionData().getDataSource());

					try {
						KubernetesClient k8sClient = new KubernetesClient();
						k8sClient.deleteJob(jobToTerminate.getVersionData().getDataSource());
					} catch (JobNotFoundException e) {
						logger.warn(e.getMessage());
					}

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