package scray.common.properties;

public class IntProperty extends Property<Integer, Integer> {
	
	private String name = null;
	private Integer defaultValue = null;

	public IntProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<Integer>() {
			public boolean checkConstraint(Integer value) {
				return value instanceof Integer;
			}
		});
	}

	public IntProperty(String name, int defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public Integer getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public Integer fromString(String string) {
		return new Integer(string);
	}

	@Override
	public boolean checkConstraintsOnValue(Object value) {
		return value instanceof Integer;
	}
}
