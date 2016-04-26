package scray.common.properties;

public class DoubleProperty extends Property<Double, Double> {
	
	private String name = null;
	private Double defaultValue = null;

	public DoubleProperty(String name) {
		this.name = name;
		super.addConstraint(new PropertyConstraint<Double>() {
			public boolean checkConstraint(Double value) {
				return value instanceof Double;
			}
		});
	}

	public DoubleProperty(String name, double defaultValue) {
		this(name);
		this.defaultValue = defaultValue;
	}
	
	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public Double getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}
	
	@Override
	public Double fromString(String string) {
		return new Double(string);
	}

	@Override
	public boolean checkConstraintsOnValue(Object value) {
		return value instanceof Double;
	}
}
