package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyMissingException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyMissingException(String propertyname) {
		super(ExceptionIDs.PROPERTY_MISSING.getName(), "Configuration property " + propertyname + " is unknown and has not been registered!");
	}
}
