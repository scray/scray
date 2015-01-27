package scray.common.properties;

import scray.common.exceptions.ScrayException;

public abstract class PropertyException extends ScrayException {

	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PropertyException.class);
	
	public PropertyException(String id, String msg) {
		super(id, msg);
		log.error("Property system reported error:", this);
	}

	private static final long serialVersionUID = 1L;
}