package scray.common.errorhandling;

public enum Severity {

	SHUTDOWN("SHUTDOWN"),
	NOTIFY("NOTIFY"),
	IGNORE("IGNORE");
	
	private String severity;
	private Severity(String severity) {
		this.severity = severity;
	}
	
	public String getSeverity() {
		return severity;
	}
}
