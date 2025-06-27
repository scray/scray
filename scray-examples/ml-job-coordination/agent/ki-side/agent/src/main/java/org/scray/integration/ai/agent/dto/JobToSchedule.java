package org.scray.integration.ai.agent.dto;

public class JobToSchedule {
	private VersionedData2 versionData;
	private AiJobsData aiJobsData;
	private K8sParameters k8sParameters;

	public JobToSchedule(VersionedData2 versionData, AiJobsData aiJobsData) {
		super();
		this.versionData = versionData;
		this.aiJobsData = aiJobsData;
	}

	public K8sParameters getK8sParameters() {
		return k8sParameters;
	}

	public void setK8sParameters(K8sParameters k8sParameters) {
		this.k8sParameters = k8sParameters;
	}

	public void setVersionData(VersionedData2 versionData) {
		this.versionData = versionData;
	}

	public void setAiJobsData(AiJobsData aiJobsData) {
		this.aiJobsData = aiJobsData;
	}

	public VersionedData2 getVersionData() {
		return versionData;
	}

	public AiJobsData getAiJobsData() {
		return aiJobsData;
	}


}
