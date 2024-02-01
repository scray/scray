package org.scray.integration.ai.agent.conf;

import java.util.HashMap;

public class Environment{
	private String name;
    private String id;
    private String ingressTemplate;
    private String k8sJobDescriptonTemplate;
    private String k8sDeploymentDescriptionTemplate;
	private EnvType type;
	private HashMap<String, String> envVars = new HashMap<>();


	public enum EnvType {
		K8s,
		Standalone,
		App
	}

	public Environment() {
		super();
	}

	public Environment(String name, String id, EnvType type) {
		super();
		this.name = name;
		this.id = id;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getIngressTemplate() {
		return ingressTemplate;
	}

	public void setIngressTemplate(String ingressTemplate) {
		this.ingressTemplate = ingressTemplate;
	}

	public String getK8sJobDescriptonTemplate() {
		return k8sJobDescriptonTemplate;
	}

	public void setK8sJobDescriptonTemplate(String k8sJobDescriptonTemplate) {
		this.k8sJobDescriptonTemplate = k8sJobDescriptonTemplate;
	}

	public String getK8sDeploymentDescriptionTemplate() {
		return k8sDeploymentDescriptionTemplate;
	}

	public void setK8sDeploymentDescriptionTemplate(String k8sDeploymentDescriptionTemplate) {
		this.k8sDeploymentDescriptionTemplate = k8sDeploymentDescriptionTemplate;
	}

	public EnvType getType() {
		return type;
	}

	public void setType(EnvType type) {
		this.type = type;
	}

	public HashMap<String, String> getEnvVars() {
		return envVars;
	}

	public void putEnvVar(String name, String value) {
		this.envVars.put(name, value);
	}
}
