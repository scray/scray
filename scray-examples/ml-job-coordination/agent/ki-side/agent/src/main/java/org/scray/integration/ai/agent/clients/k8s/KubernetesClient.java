
package org.scray.integration.ai.agent.clients.k8s;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.fabric8.kubernetes.api.builder.Fluent;
import io.fabric8.kubernetes.api.builder.Visitor;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.apps.DeploymentFluent.MetadataNested;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.api.model.extensions.IngressRuleBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
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
		var descriptor = aiK8client.loadDesciptorFormFile("job2.yaml"); // Fixme add path parameter

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

	public void deployAppJob(String jobName, String imageName) {
		KubernetesClient aiK8client = new KubernetesClient();

		var deploymentName = jobName;
		var descriptor = aiK8client.loadDesciptorFormFile("job2.yaml"); // Fixme

		if(deploymentName == null) {
			logger.error("Deploymentdescriptor (job2.yaml) not found");
		} else {

			var configureJob = aiK8client.configureJobDescriptor(
					descriptor,
					deploymentName,
					jobName,
					imageName);

			var configuredVpc = aiK8client.configurePvc(descriptor, deploymentName);

			try {

				aiK8client.deployJob(configureJob);
			} catch(Exception e) {
				logger.warn("Error while creating job. {}", e );
			}
			try {
				aiK8client.deployVolumeClaim(configuredVpc);
			} catch(Exception e) {
				logger.warn("Error while creating pvc. {}", e );
			}

			aiK8client.close();
		}
	}

	public void deployApp(String jobName, String imageName, String jobTemplatePath) {

		String host = jobName + ".app.research.dev.seeburger.de";
		String ingressPath = "/";
		String serviceName = jobName;
		int portNumber = 7860;


		var serviceDescriptorTemplate = this.loadDesciptorFormFile("service.yaml");
		Service serviceDescription = this.configureServiceDefinion(serviceDescriptorTemplate, serviceName, jobName, portNumber);

		System.out.println(serviceDescription);

		this.deployService(serviceDescription);

		// Configure ingress
		var ingressDescriptorTemplate = this.loadDesciptorFormFile("app-ingress.yaml");
		Ingress ingressDescription = this.configureIngressDefinition(ingressDescriptorTemplate, host, jobName, ingressPath, serviceName, portNumber);

		this.deployIngress(ingressDescription);

		try {
			this.deployIngress(ingressDescription);
		} catch(IllegalArgumentException e) {
			if(e.getLocalizedMessage().contains("Nothing to create.")) {
				logger.info("Ingress exists. Noting to create");
			} else {
				throw e;
			}
		}


		this.deployJob(jobName, imageName, jobTemplatePath);

	}

	public void deployJob(String jobName, String imageName, String jobTemplatePath) {
		KubernetesClient aiK8client = new KubernetesClient();

		var deploymentName = jobName;
		var descriptor = aiK8client.loadDesciptorFormFile(jobTemplatePath);

		if(descriptor == null) {
			logger.error("Deploymentdescriptor (" + jobTemplatePath + ") not found");
		} else {

			var configureJob = aiK8client.configureJobDescriptor(
					descriptor,
					deploymentName,
					jobName,
					imageName);

			var configuredVpc = aiK8client.configurePvc(descriptor, deploymentName);

			try {
				aiK8client.deployJob(configureJob);
			} catch(Exception e) {
				logger.warn("Error while creating job. {}", e );
			}
			try {
				aiK8client.deployVolumeClaim(configuredVpc);
			} catch(Exception e) {
				logger.warn("Error while creating pvc. {}", e );
			}

			aiK8client.close();
		}
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
			logger.error("Deployment descriptor file not found {}", path);
		} catch (IOException e) {
			logger.error("Error while reading deployment descrptor file {}", path);
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

	public Job configureJobDescriptor(
			NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> preparedDeploymentDescriptor,
			String deplymentName,
			String jobName,
			String imageName) {

		var jobDescription = preparedDeploymentDescriptor.items().stream()
			.filter(item -> item != null && item.getKind().equals("Job") &&  item.getMetadata().getName().equals("jupyter-tensorflow-job"))
			.map(Job.class::cast)
			.map(job -> {
				Job newDeplyment = new JobBuilder(job)
						.withNewMetadata()
							.withName(deplymentName)
							.removeFromLabels("app")
							.addToLabels("app", deplymentName)
						.endMetadata()
						.editOrNewSpec()
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
									.editMatchingEnv(e -> e.getName().equals("RUN_TYPE")).withValue("once").endEnv()
									.endContainer()
								.endSpec()
							.endTemplate()
						.endSpec()
					.build();

				return newDeplyment;
			})
			.reduce(null, (a, b) -> b);

			return jobDescription;
	}


	public Ingress configureIngressDefinition(
			NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> preparedDeploymentDescriptor,
			String host,
			String jobName,
			String path,
			String serviceName,
			int portNumber) {

			return preparedDeploymentDescriptor.items().stream()
			.filter(item -> item != null && item.getKind().equals("Ingress") &&  item.getMetadata().getName().equals("scray-app-ingress-template"))
			.map(Ingress.class::cast)
			.map(ingress -> {
				Ingress newDeplyment = new IngressBuilder(ingress)
						.withNewMetadata()
							.withName(jobName)
							.addToAnnotations("nginx.org/websocket-services", jobName)
						.endMetadata()
						.editOrNewSpec()
							.editMatchingRule(r -> r.getHost().equals("HOST"))
								.withHost(host)
								.editHttp()
									.editMatchingPath(p -> p.getPath().equals("/"))
									.withPath(path)
									.editBackend()
										.editOrNewService()
											.withName(serviceName)
											.editOrNewPort()
												.withNumber(portNumber)
											.endPort()
										.endService()
									.endBackend()
									.endPath()
								.endHttp()
							.endRule()
						.endSpec()
					.build();

				return newDeplyment;
			})
			.reduce(null, (a, b) -> b);
	}

	public Service configureServiceDefinion(
			NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable<HasMetadata> preparedDeploymentDescriptor,
			String selectorApp,
			String serviceName,
			Integer port
			) {

		return preparedDeploymentDescriptor.items().stream()
			.filter(item -> item != null && item.getKind().equals("Service") &&  item.getMetadata().getName().equals("SCARY-APP-SERVICE"))
			.map(Service.class::cast)
			.map(ingress -> {
				Service newDeplyment = new ServiceBuilder(ingress)
						.withNewMetadata()
							.withName(serviceName)
						.endMetadata()
						.editOrNewSpec()
							.addToSelector("app", selectorApp)
							.editFirstPort()
								.withPort(port)
							.endPort()
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
				.filter(item -> item != null && item.getKind().equals("persistentVolumeClaim") &&  item.getMetadata().getName().equals("notebooks-pv-claim"))
				.map(PersistentVolumeClaim.class::cast)
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

	public void deployJob(Job job) {
		this.client.batch().v1().jobs().inNamespace("default").resource(job).delete();
		this.client.batch().v1().jobs().inNamespace("default").resource(job).serverSideApply();
	}

	public void deployDeployment(Deployment dep) {
		this.client.apps().deployments().inNamespace("default").resource(dep).delete();
		this.client.apps().deployments().inNamespace("default").resource(dep).serverSideApply();
	}

	public void deployVolumeClaim(PersistentVolumeClaim pvclaim) {
		client.persistentVolumeClaims().inNamespace("default").createOrReplace(pvclaim);
	}

	public void deployIngress(Ingress ingress) {
		client.network().v1().ingresses().inNamespace("default").createOrReplace(ingress);
	}

	public void deployService(Service service) {
		client.services().inNamespace("default").createOrReplace(service);
	}

	public void deleteJob(String jobName) {
		Job job = client.batch().v1().jobs().inNamespace("default").list().getItems().stream()
		.filter(jobR -> {
			String appName = jobR.getSpec().getTemplate().getMetadata().getLabels().get("app");
			return appName.equals(jobName);
		})
		.reduce(null, (a, b) -> b);

		if(job != null) {
			logger.info("Delete job {}", jobName);
			this.client.batch().v1().jobs().inNamespace("default").resource(job).delete();
		} else {
			logger.info("Can not delete job {}. Job not found", jobName);
			throw new JobNotFoundException("Can not delete job " + jobName + ". Job not found");
		}
	}

	public void close() {
		this.client.close();
	}

	public class JobNotFoundException
	  extends RuntimeException {
	    public JobNotFoundException(String errorMessage) {
	        super(errorMessage);
	    }
	}
}
