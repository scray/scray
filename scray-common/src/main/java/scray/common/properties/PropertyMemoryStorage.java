package scray.common.properties;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * stores properties in memory
 * 
 * @author andreas
 *
 */
public class PropertyMemoryStorage implements PropertyStoragePuttable {

	private Map<String, Object> props = new HashMap<String, Object>();

	public PropertyMemoryStorage(Map<String, Object> initialProperties) {
		props.putAll(initialProperties);
	}

	public PropertyMemoryStorage() {
	}

	@Override
	public <T, U> T get(Property<T, U> name) {
		return name.transformToStorageFormat((U) props.get(name.getName()));
	}

	@Override
	public <T, U> void put(Property<T, U> name, T value) {
		props.put(name.getName(), name.toString(value));
	}

	@Override
	public void init() {
		// check initial properties
		checkProperties(this);
	}

	public String toString() {
		return "PropertyMemoryStorage, size=" + props.size();
	}

	@SuppressWarnings("unchecked")
	public static void checkProperties(PropertyMemoryStorage propStore) {
		for (Entry<String, Object> entry : propStore.props.entrySet()) {
			for (Property<?, ?> prop : (Collection<Property<?, ?>>) (Object) ScrayProperties
					.getRegisteredProperties()) {
				if (prop.getName().equals(entry.getKey())) {					
					if (!prop.checkConstraintsOnValue(entry.getValue())) {
						throw new RuntimeException(
								new PropertyConstraintViolatedException(entry.getKey()));
					}
				}
			}
		}
	}
}
