package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyExistsException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyExistsException(String propertyname) {
		super(ExceptionIDs.PROPERTY_EXISTS.getName(), "Configuration property " + propertyname + " has already been registered!");
	}
}
