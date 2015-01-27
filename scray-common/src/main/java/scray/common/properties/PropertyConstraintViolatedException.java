package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyConstraintViolatedException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyConstraintViolatedException(String propertyname) {
		super(ExceptionIDs.PROPERTY_CONSTRAINT.getName(), "Constraint violated for configuration property " + propertyname + "!");
	}
}
