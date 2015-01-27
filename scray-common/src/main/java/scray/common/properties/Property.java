package scray.common.properties;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Property<T, U> {

	private List<PropertyConstraint<T>> constraints = new ArrayList<PropertyConstraint<T>>();
	private Set<PropertyChangeListener> listeners = new HashSet<PropertyChangeListener>();
	
	public boolean checkConstraints(T value) {
		for(PropertyConstraint<T> constraint: constraints) {
			if(!constraint.checkConstraint(value)) {
				return false; 
			}
		}
		return true;
	}
	
	protected void commitChange(T value, T oldvalue) {
		PropertyChangeEvent propChange = new PropertyChangeEvent(this, getName(), oldvalue, value);
		for(PropertyChangeListener listener: listeners) {
			listener.propertyChange(propChange);
		}
	}
	
	public void addConstraint(PropertyConstraint<T> constraint) {
		constraints.add(constraint);
	}
	
	public void addPropertyChangeListener(PropertyChangeListener listener) {
		listeners.add(listener);
	}

	public void removeListener(PropertyChangeListener listener) {
		listeners.remove(listener);
	}
	
	public Set<PropertyChangeListener> getPropertyChangeListeners() {
		return listeners;
	}
	
	@SuppressWarnings("unchecked")
	public U transformToResult(T value) {
		return (U)value;
	}

	@SuppressWarnings("unchecked")
	public T transformToStorageFormat(U value) {
		return (T)value;
	}

	@SuppressWarnings("unchecked")
	public T fromString(String value) {
		return (T)value;
	}

	public abstract boolean hasDefault();
	
	public abstract U getDefault();
	
	public abstract String getName();
	
}
