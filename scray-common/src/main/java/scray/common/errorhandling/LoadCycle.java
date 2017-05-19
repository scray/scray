package scray.common.errorhandling;

public enum LoadCycle {

	FIRST_LAOD("FIRST_RELAOD"),
	RELOAD("RELOAD"),
	RUNTIME("RUNTIME");
	
	private String loadcycle;
	private LoadCycle(String loadcycle) {
		this.loadcycle = loadcycle;
	}
	
	public String getLoadCycle() {
		return loadcycle;
	}
}
