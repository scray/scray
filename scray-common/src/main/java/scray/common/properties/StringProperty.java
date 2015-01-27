package scray.common.properties;

import java.util.regex.Pattern;

public class StringProperty extends Property<String, String> {

	private String name = null;
	private String defaultValue = null;

	public StringProperty(String name) {
		this.name = name;
	}

	public StringProperty(String name, String defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public String getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	public void addRegexConstraint(final String pattern) {
		super.addConstraint(new PropertyConstraint<String>() {
			Pattern patMatch = Pattern.compile(pattern);
			@Override
			public boolean checkConstraint(String value) {
				return patMatch.matcher(pattern).find();
			}
		});
	}
}
