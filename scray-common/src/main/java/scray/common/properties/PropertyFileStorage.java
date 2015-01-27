package scray.common.properties;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Storage abstraction for property files
 * @author andreas
 *
 */
public class PropertyFileStorage implements PropertyStorage {

	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PropertyFileStorage.class);

	private String location = null;
	private String systemEnv = null;

	public PropertyFileStorage(String propertyFileLocation, String systemEnvPathArgument) {
		systemEnv = systemEnvPathArgument;
		location = propertyFileLocation;
	}
	
	private Properties props;

	/**
	 * get the property value out of the 
	 */
	public <T, U> T get(Property<T, U> name) {
		return name.fromString((String)props.get(name.getName()));
	}
	
	/**
	 * Properties in property files cannot be set
	 */
	public <T, U> void put(Property<T, U> name, T value) {}
	
	/**
	 * Initializes system with default property file
	 */
	@SuppressWarnings("unchecked")
	public void init() {
		// check if there is a command line argument set, otherwise use default argument
		props = new Properties();
		try {
			if(System.getProperty(systemEnv) != null) {
				props.load(ScrayProperties.class.getClassLoader()
						.getResourceAsStream(System.getProperty(systemEnv)));
				
			} else {
				props.load(ScrayProperties.class.getClassLoader()
					.getResourceAsStream(location));
			}
		} catch (IOException e) {
			log.error("Failed to load " + location + "", e);
		}
		for(Entry<Object, Object> entry: props.entrySet()) {
			// verify that all registered properties have the right type 
			// (others might have not been registered because some module is not in use)
			String key = (String)entry.getKey();
			String value = (String)entry.getValue();
			for(Property<Object, Object> prop: (Collection<Property<Object, Object>>)(Object)ScrayProperties.getRegisteredProperties()) {
				if(prop.getName().equals(key)) {
					if(!prop.checkConstraints(prop.fromString(value))) {
						throw new RuntimeException(new PropertyConstraintViolatedException(key));
					}
				}
			}
		}
	} 
	
	/**
	 * Properties in property files cannot be set
	 */
	public boolean isUpdatableStore() {
		return true;
	}

	public static String DEFAULT_PROPERTY_FILE = "scray.properties";
	public static String DEFAULT_JVM_ARGUMENT = "scray.properties"; 
}
