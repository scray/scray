package scray.common.properties;

import java.util.Map;
import java.util.Properties;

/**
 * stores properties in memory
 * @author andreas
 *
 */
public class PropertyMemoryStorage implements PropertyStoragePuttable {

	private Properties props = new Properties();
	
	public PropertyMemoryStorage(Map<String, String> initialProperties) {
		props.putAll(initialProperties);
	}

	public PropertyMemoryStorage() {}
	
	@Override
	public <T, U> T get(Property<T, U> name) {
		return name.fromString((String)props.get(name.getName()));
	}

	@Override
	public <T, U> void put(Property<T, U> name, T value) {
		props.put(name.getName(), name.toString(value));
	}

	@Override
	public void init() {
		// check initial properties
		PropertyFileStorage.checkProperties(props);
	}
	
	public String toString() {
		return "PropertyMemoryStorage, size=" + props.size();
	}
}
