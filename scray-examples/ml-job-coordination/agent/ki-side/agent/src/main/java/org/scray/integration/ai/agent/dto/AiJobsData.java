package org.scray.integration.ai.agent.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AiJobsData {

	String state;
	String filename;
	String dataDir;
	String notebookName;
	String imageName =     "huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2";
	String processingEnv = "http://scray.org/ai/jobs/env/see/ki1-k8s";

	public String getProcessingEnv() {
		return processingEnv;
	}

	public void setProcessingEnv(String processingEnv) {
		this.processingEnv = processingEnv;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public String getDataDir() {
		return dataDir;
	}

	public void setDataDir(String dataDir) {
		this.dataDir = dataDir;
	}

	public String getNotebookName() {
		return notebookName;
	}

	public void setNotebookName(String notebookName) {
		this.notebookName = notebookName;
	}

	public String getImageName() {
		return imageName;
	}

	public void setImageName(String imageName) {
		this.imageName = imageName;
	}

	public AiJobsData() {

	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

}
