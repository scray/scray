package scray.common.properties;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scray.common.tools.ScrayCredentials;

/**
 * Represents a property with username and password.
 * Credentials are represented as "&lt;username&gt;":"&lt;password&gt;" 
 * @author andreas
 *
 */
public class CredentialsProperty extends Property<String, ScrayCredentials> {

	private Pattern credentialsPattern = Pattern.compile("\"(.*)\":\"(.*)\"");
	
	private String name = null;
	private ScrayCredentials defaultValue = null;

	public CredentialsProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<String>() {
			public boolean checkConstraint(String value) {
				if(!(value instanceof String)) {
					return false;
				} else {
					if(value == null || value.trim().equals("")) {
						return true;
					} else {
						Matcher matcher = credentialsPattern.matcher(value);
						return matcher.matches();
					}
				}
			}
		});
	}

	public CredentialsProperty(String name, ScrayCredentials defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public ScrayCredentials getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public ScrayCredentials transformToResult(String value) {
		if(value == null || value.trim().equals("")) {
			return new ScrayCredentials(null, null);
		}
		Matcher matcher = credentialsPattern.matcher(value);
		ScrayCredentials result = null;
		if(matcher.matches()) {
			result = new ScrayCredentials(matcher.group(1), matcher.group(2).toCharArray());
		} 
		return result;
	}
	
	@Override
	public String transformToStorageFormat(ScrayCredentials value) {
		if(value == null || value.isEmpty()) {
			return "";
		}
		StringBuilder sb = new StringBuilder();
		sb.append('"');
		sb.append(value.getUsername());
		sb.append("\":\"");
		sb.append(value.getPassword());
		sb.append('"');
		return sb.toString();
	}

	@Override
	public boolean checkConstraintsOnValue(Object value) {
		return value instanceof ScrayCredentials;
	}
}
