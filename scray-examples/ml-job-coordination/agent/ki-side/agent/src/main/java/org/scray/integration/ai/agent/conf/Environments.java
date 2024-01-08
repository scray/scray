package org.scray.integration.ai.agent.conf;

import java.util.List;

public class Environments {
    public String version;
    public String name;
    public List<Environment> environments;


	public Environments() {
		super();
	}

	public Environments(String version, String name, List<Environment> environments) {
		super();
		this.version = version;
		this.name = name;
		this.environments = environments;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<Environment> getEnvironments() {
		return environments;
	}

	public void setEnvironments(List<Environment> environments) {
		this.environments = environments;
	}

}
