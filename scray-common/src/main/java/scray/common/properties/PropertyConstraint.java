package scray.common.properties;

public abstract class PropertyConstraint<T> {

	public abstract boolean checkConstraint(T value);
}
