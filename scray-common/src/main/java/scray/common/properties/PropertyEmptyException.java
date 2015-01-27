package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyEmptyException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyEmptyException(String propertyname) {
		super(ExceptionIDs.PROPERTY_EMPTY.getName(), "Configuration property " + propertyname + " has not been set previously!");
	}
}
