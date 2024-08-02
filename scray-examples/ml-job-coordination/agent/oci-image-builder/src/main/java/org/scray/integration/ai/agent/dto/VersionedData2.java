package org.scray.integration.ai.agent.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class VersionedData2 {
	
	String dataSource;
	String data;
	String mergeKey;
	String version;
	String versionKey;
	

	public String getVersionKey() {
		return versionKey;
	}

	public void setVersionKey(String versionKey) {
		this.versionKey = versionKey;
	}

	public String getMergeKey() {
		return mergeKey;
	}

	public void setMergeKey(String mergeKey) {
		this.mergeKey = mergeKey;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public VersionedData2() {
		
	}

}
