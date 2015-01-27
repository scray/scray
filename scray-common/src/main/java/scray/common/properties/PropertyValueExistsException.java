package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyValueExistsException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyValueExistsException(String propertyname) {
		super(ExceptionIDs.PROPERTY_VALUE_EXISTS.getName(), "Configuration property " + propertyname + " is already existing and may not be overwritten!");
	}
}
