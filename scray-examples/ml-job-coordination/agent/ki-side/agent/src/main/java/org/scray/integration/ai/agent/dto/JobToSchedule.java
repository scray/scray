package org.scray.integration.ai.agent.dto;

public class JobToSchedule {
	private VersionedData2 versionData;
	private AiJobsData aiJobsData;
	
	public JobToSchedule(VersionedData2 versionData, AiJobsData aiJobsData) {
		super();
		this.versionData = versionData;
		this.aiJobsData = aiJobsData;
	}
	
	public VersionedData2 getVersionData() {
		return versionData;
	}
	
	public AiJobsData getAiJobsData() {
		return aiJobsData;
	}
	

}
