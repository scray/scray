package org.scray.integration.ai.agent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import scray.sync.api.VersionedData;

import org.scray.integration.ai.agent.clients.k8s.KubernetesClient;
import org.scray.integration.ai.agent.clients.rest.RestClient;
import org.scray.integration.ai.agent.dto.AiJobsData;
import org.scray.integration.ai.agent.dto.VersionedData2;
import org.slf4j.Logger;

public class AiIntegrationAgent {

	private static final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);

	public AiIntegrationAgent() {

	}

	public static void main(String[] args) throws InterruptedException {
		var agent = new AiIntegrationAgent();

		while (!Thread.currentThread().isInterrupted()) {
			agent.pollForNewJobs();
			Thread.sleep(5000);
		}
	}

	public void pollForNewJobs() {
		var apiClient = new RestClient();

		try {

			ObjectMapper mapper = new ObjectMapper();

			String data = apiClient.getData();

			List<VersionedData2> vd = Arrays.asList(mapper.readValue(data, VersionedData2[].class));

			for (VersionedData2 versionedData2 : vd) {
				AiJobsData aiJobState = mapper.readValue(versionedData2.getData(), AiJobsData.class);

				// if (aiJobState.getState().equals("UPLOADED")) {
				if (aiJobState.getState().equals("UPLOADED")) {
					if (aiJobState.getProcessingEnv() != null
							&& aiJobState.getProcessingEnv().equals("http://scray.org/ai/jobs/env/see/ki1-k8s")) {

						logger.info("Schedule container for {}", versionedData2.getDataSource());

						aiJobState.setState("SCHEDULED");
						versionedData2.setData(mapper.writeValueAsString(aiJobState));

						String newOutput = mapper.writeValueAsString(versionedData2);

						logger.info("Writ to API {}", newOutput);
						apiClient.putData(newOutput);

						KubernetesClient k8sClient = new KubernetesClient();
						k8sClient.DeployJob(versionedData2.getDataSource());

					} else {
						logger.debug("Job which is not for my env {}", aiJobState.getProcessingEnv());
					}

				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}