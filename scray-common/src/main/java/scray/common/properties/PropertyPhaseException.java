package scray.common.properties;

import scray.common.exceptions.ExceptionIDs;
import scray.common.properties.ScrayProperties.Phase;

public class PropertyPhaseException extends PropertyException {

	private static final long serialVersionUID = -1L;

	public PropertyPhaseException(Phase newPhase, Phase oldPhase) {
		super(ExceptionIDs.PROPERTY_PHASE.getName(), "Property phase " + newPhase + " must be higher than " + oldPhase);
	}
	
	public PropertyPhaseException(Phase phase) {
		super(ExceptionIDs.PROPERTY_PHASE.getName(), "Wrong phase! Phase should be " + phase);
	}

	public PropertyPhaseException(Phase phase, Property<?, ?> prop) {
		super(ExceptionIDs.PROPERTY_PHASE.getName(), "Wrong phase! Phase should be " + phase + " with property " + prop.getName());
	}

}
