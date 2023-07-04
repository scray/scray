package org.scray.integration.ai.agent.clients.k8s;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.builder.Fluent;
import io.fabric8.kubernetes.api.builder.Visitor;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentFluent.MetadataNested;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;

public class KubernetesClient {

	private static Logger logger = LoggerFactory.getLogger(KubernetesClient.class);
	DefaultKubernetesClient client = null;

	public KubernetesClient() {
		this.client = new DefaultKubernetesClient();
	}

	public static void main(String[] args) throws IOException {

		KubernetesClient aiK8client = new KubernetesClient();
		
		var deploymentName = "scray-ai-job-" + UUID.randomUUID();
		var descriptor = aiK8client.loadDesciptorFormFile("job.yaml"); // Fixme add path parameter
				
		var configuredDescriptor = aiK8client.configureDeploymentDescriptor(
				descriptor, 
				deploymentName, 
				deploymentName, 
				"huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2");
		
		var configuredVpc = aiK8client.configurePvc(descriptor, deploymentName);
		
		aiK8client.deployDeployment(configuredDescriptor);
		aiK8client.deployVolumeClaim(configuredVpc);
		
		aiK8client.close();
	}
	
	public void DeployJob(String jobName) {
		KubernetesClient aiK8client = new KubernetesClient();
		
		var deploymentName = "scray-ai-job-" + UUID.randomUUID();
		var descriptor = aiK8client.loadDesciptorFormFile("job.yaml"); // Fixme
				
		var configuredDescriptor = aiK8client.configureDeploymentDescriptor(
				descriptor, 
				deploymentName, 
				jobName, 
				"huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2");
		
		var configuredVpc = aiK8client.configurePvc(descriptor, deploymentName);
		
		aiK8client.deployDeployment(configuredDescriptor);
		aiK8client.deployVolumeClaim(configuredVpc);
		
		aiK8client.close();
	}
	
	public NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> loadDesciptorFormFile(String path) {
		FileInputStream jobDeploymentDescriptor;
		NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata>  preparedDeploymentDescriptor = null;
		
		try {
			jobDeploymentDescriptor = new FileInputStream(new File(path));
			 preparedDeploymentDescriptor = client
					.inNamespace("default")
					.load(jobDeploymentDescriptor);
				jobDeploymentDescriptor.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return preparedDeploymentDescriptor;
		
	}
	
	public Deployment configureDeploymentDescriptor(
			NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> preparedDeploymentDescriptor, 
			String deplymentName, 
			String jobName, 
			String imageName) {
			
			return preparedDeploymentDescriptor.items().stream()
			.filter(item -> item != null && item.getKind().equals("Deployment") &&  item.getMetadata().getName().equals("jupyter-tensorflow-job"))
			.map(deplyment -> (Deployment)deplyment)
			.map(deployment -> {
				Deployment newDeplyment = new DeploymentBuilder(deployment)
						.withNewMetadata()
							.withName(deplymentName)
							.addToLabels("app", deplymentName)
						.endMetadata()
						.editOrNewSpec()
							.withNewSelector()
								.addToMatchLabels("app", deplymentName)
							.endSelector()
							.editOrNewTemplate()
								.withNewMetadata()
									.addToLabels("app", deplymentName)
								.endMetadata()
								.editOrNewSpec()
									.editMatchingContainer(c -> c.getName().equals("scray-ai-container"))
									.withImage(imageName)
									.editMatchingEnv(e -> e.getName().equals("JOB_NAME"))
										.withValue(jobName)
									.endEnv()
									.endContainer()
								.endSpec()
							.endTemplate()
						.endSpec()
					.build();
				
				return newDeplyment;
			})
			.reduce(null, (a, b) -> b);
	}
	
	public PersistentVolumeClaim configurePvc(
			NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> preparedDeploymentDescriptor,
			String deplymentName) {
		
		return preparedDeploymentDescriptor.items().stream()
				.filter(item -> item != null && item.getKind().equals("PersistentVolumeClaim") &&  item.getMetadata().getName().equals("notebooks-pv-claim"))
				.map(pvc -> (PersistentVolumeClaim)pvc)
				.map(pvc -> {
					
					logger.debug("Update pvc ");
					return new PersistentVolumeClaimBuilder(pvc)
					.withNewMetadata()
					.withName(deplymentName)
					.endMetadata()
					.build();
				})
				.reduce(null, (a, b) -> b);
	}
	
	public void deployDeployment(Deployment dep) {
		this.client.apps().deployments().inNamespace("default").createOrReplace(dep);
	}
	
	public void deployVolumeClaim(PersistentVolumeClaim pvclaim) {
		client.persistentVolumeClaims().inNamespace("default").createOrReplace(pvclaim);
	}
	
	public void close() {
		this.client.close();
	}

}
