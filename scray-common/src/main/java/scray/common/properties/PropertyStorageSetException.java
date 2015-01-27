package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;

public class PropertyStorageSetException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyStorageSetException() {
		super(ExceptionIDs.PROPERTY_STORAGE.getName(), "Configuration property store has already been registered!");
	}
}
