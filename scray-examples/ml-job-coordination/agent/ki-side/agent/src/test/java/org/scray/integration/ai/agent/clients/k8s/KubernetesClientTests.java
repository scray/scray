package org.scray.integration.ai.agent.clients.k8s;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;

public class KubernetesClientTests {

	@Test
	public void configureIngressDescription() {
		KubernetesClient aiK8client = new KubernetesClient();

		var descriptor = aiK8client.loadDesciptorFormFile("src/test/resources/k8s/app-ingress.yaml");


		Ingress ingressDescription = aiK8client.configureIngressDefinition(descriptor, "research.ibm.com", "job2", "/app-frieda", "friedas-service", 4711);

		ObjectMapper objectMapper = new ObjectMapper();
		try {
			//aiK8client.deployIngress(ingressDescription);

			System.out.println(objectMapper.writeValueAsString(ingressDescription));
		} catch (JsonProcessingException e) {
            Assertions.fail(e);
		}

	}
	@Test
	public void configureService() {
		KubernetesClient aiK8client = new KubernetesClient();

		var descriptor = aiK8client.loadDesciptorFormFile("src/test/resources/k8s/service.yaml");
		Service serviceDescription = aiK8client.configureServiceDefinion(descriptor, "scray-app1", "scray-app1-service", 7411);


		ObjectMapper objectMapper = new ObjectMapper();
		try {
		    String serviceDef = objectMapper.writeValueAsString(serviceDescription);

		    Assertions.assertTrue(serviceDef.contains("{\"name\":\"scray-app1-service\"}"));
            Assertions.assertTrue(serviceDef.contains("\"selector\":{\"app\":\"scray-app1\"}"));
		} catch (JsonProcessingException e) {
            Assertions.fail(e);
		}
	}

	@Test
	public void configureAppDeployment() {
		KubernetesClient aiK8client = new KubernetesClient();

		var descriptor = aiK8client.loadDesciptorFormFile("src/test/resources/k8s/service.yaml");
		Service serviceDescription = aiK8client.configureServiceDefinion(descriptor, "scray-app1", "scray-app1", 7411);


		//aiK8client.deployService(serviceDescription);

		ObjectMapper objectMapper = new ObjectMapper();
		try {
			System.out.println(objectMapper.writeValueAsString(serviceDescription));
		} catch (JsonProcessingException e) {
            Assertions.fail(e);
		}
	}

	@Test
	public void configureJobDescription() {
		KubernetesClient aiK8client = new KubernetesClient();

		var descriptor = aiK8client.loadDesciptorFormFile("src/test/resources/k8s/app-job.yaml");
		Job jobDescription = aiK8client.configureJobDescriptor(descriptor, "scray-app1", "scray-app1", "PYTHON","image1", "http://ml-integration.research.dev.example.com:8082", "ml-integration.research.dev.example.com");

		ObjectMapper objectMapper = new ObjectMapper();
		String jobDef;
        try
        {
            jobDef = objectMapper.writeValueAsString(jobDescription);
            Assertions.assertTrue(jobDef.contains("{\"name\":\"RUNTIME_TYPE\",\"value\":\"PYTHON\"}"));
            Assertions.assertTrue(jobDef.contains("{\"name\":\"SCRAY_DATA_INTEGRATION_HOST\",\"value\":\"ml-integration.research.dev.example.com\"}"));
            Assertions.assertTrue(jobDef.contains("{\"name\":\"SCRAY_SYNC_API_URL\",\"value\":\"http://ml-integration.research.dev.example.com:8082\"}"));
        }
        catch (JsonProcessingException e)
        {
            Assertions.fail(e);
        }


	}




}
