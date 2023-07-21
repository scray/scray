package org.scray.integration.ai.agent.dto;

public class Environment {
	
	public enum EnvType {
		K8s,
		Standalone
	}
	
	private String name;
	private EnvType type;
	

	public EnvType getType() {
		return type;
	}

	public void setType(EnvType type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
