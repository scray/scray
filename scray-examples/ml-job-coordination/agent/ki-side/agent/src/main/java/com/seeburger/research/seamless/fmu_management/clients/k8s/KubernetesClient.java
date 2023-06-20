package com.seeburger.research.seamless.fmu_management.clients.k8s;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;

public class KubernetesClient {

	private static Logger logger = LoggerFactory.getLogger(KubernetesClient.class);

	public static void main(String[] args) throws IOException {
		String deploymentName = "nginx-testdeployment";
		String image = "nginx:1.7.9";
		String container = "nginx";

		KubernetesClient client = new KubernetesClient();

		// client.addDeployment(deploymentName, image, container);
		// client.printPods();
		// client.getUUIDInformation();
		// client.getDeploymentInformations();
		// KubernetesClient.deleteDeployment(deploymentName);

	}

	public void printPods() {
		Config config = new ConfigBuilder().withMasterUrl("https://rancher.dev.seeburger.de/k8s/clusters/c-m-pn7wf2rh")
				.build();
		DefaultKubernetesClient client = new DefaultKubernetesClient(config);

		client.pods().inNamespace("default").list().getItems()
				.forEach(pod -> logger.info(pod.getMetadata().getName()));
	}

	public static void addDeployment(String deploymentName, String image, String container) {
	DefaultKubernetesClient client= new DefaultKubernetesClient();
		Deployment deployment = new DeploymentBuilder()
				.withNewMetadata()
				   .withName(deploymentName)
				   .addToLabels("app", "nginx")
				.endMetadata()
				.withNewSpec()
				   .withReplicas(1)
				   .withNewSelector()
					   .addToMatchLabels("app", "nginx")
				   .endSelector()
				   .withNewTemplate()
					   .withNewMetadata()
						  .addToLabels("app", "nginx")
					   .endMetadata()
					   .withNewSpec()
						  .addNewContainer()
							  .withName(container)
							  .withImage(image)
							  .addNewPort().withContainerPort(80).endPort()
						  .endContainer()
					   .endSpec()
				   .endTemplate()
				.endSpec()
				.build();

		client.apps().deployments().inNamespace("default").createOrReplace(deployment);
	}
  }
