package org.scray.integration.ai.agent.dto;

public class K8sParameters {
	private String k8sTemplateFile = "app-job.yaml";


	public K8sParameters(String k8sTemplateFile) {
		super();
		this.k8sTemplateFile = k8sTemplateFile;
	}


	public String getK8sTemplateFile() {
		return k8sTemplateFile;
	}

	public void setK8sTemplateFile(String k8sTemplateFile) {
		this.k8sTemplateFile = k8sTemplateFile;
	}
}
