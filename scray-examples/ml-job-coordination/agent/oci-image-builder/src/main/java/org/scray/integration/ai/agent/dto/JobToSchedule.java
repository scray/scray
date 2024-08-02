package org.scray.integration.ai.agent.dto;

public class JobToSchedule {
	private VersionedData2 versionData;
	private AiJobsData aiJobsData;
	private K8sParameters k8sParameters;

	public JobToSchedule(VersionedData2 versionData, AiJobsData aiJobsData, K8sParameters k8sParameters) {
		super();
		this.versionData = versionData;
		this.aiJobsData = aiJobsData;
		this.k8sParameters = k8sParameters;
	}

	public VersionedData2 getVersionData() {
		return versionData;
	}

	public AiJobsData getAiJobsData() {
		return aiJobsData;
	}


}
