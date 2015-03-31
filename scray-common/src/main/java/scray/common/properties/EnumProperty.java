package scray.common.properties;

public class EnumProperty<X extends java.lang.Enum<X>> extends
		scray.common.properties.Property<String, X> {

	private String name;
	private X defaultValue;
	private Class<X> clazz;

	public EnumProperty(String name, X defaultValue, Class<X> clazz) {
		this.name = name;
		this.defaultValue = defaultValue;
		this.clazz = clazz;
	}

	@Override
	public boolean hasDefault() {
		return defaultValue != null;
	}

	@Override
	public X getDefault() {
		return defaultValue;
	}

	@Override
	public String getName() {
		return name;
	}

	public String toString(String value) {
		return value;
	}

	public String fromString(String value) {
		return value;
	}

	public String transformToStorageFormat(X value) {
		return value.name();
	}

	public X transformToResult(String value) {
		return Enum.valueOf(clazz, value);
	}
}
