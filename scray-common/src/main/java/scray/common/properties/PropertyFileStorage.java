package scray.common.properties;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * Storage abstraction for property files
 * 
 * @author andreas
 * 
 */
public class PropertyFileStorage implements PropertyStorage {

	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(PropertyFileStorage.class);

	private String location = null;

	public enum FileLocationTypes {
		JarPropertiesFile, LocalPropertiesFile
	}

	private FileLocationTypes fileLocationType = null;

	public PropertyFileStorage(String location,
			FileLocationTypes fileLocationType) {
		this.location = location;
		this.fileLocationType = fileLocationType;
	}

	private Properties props;

	/**
	 * get the property value out of the
	 */
	public <T, U> T get(Property<T, U> name) {
		String value = (String) props.get(name.getName());
		if (value != null) {
			return name.fromString(value);
		} else {
			return null;
		}
	}

	/**
	 * Initializes system with default property file
	 */
	public void init() {

		props = new Properties();

		try {
System.out.println("0" + location + "\n\n\n");
			if (fileLocationType.equals(FileLocationTypes.JarPropertiesFile)) {
				props.load(ScrayProperties.class.getClassLoader()
						.getResourceAsStream(location));
			} else if (fileLocationType
					.equals(FileLocationTypes.LocalPropertiesFile)) {
				System.out.println(location + "\t" + System.getProperty(location));
				if(System.getProperty(location) != null) {
					System.out.println("1" + location + "\n\n\n");
					props.load(new FileInputStream(location));
				} else {
					System.out.println("2" + location + "\t" + System.getenv(location) + "\n\n\n");
					props.load(new FileInputStream(System.getenv(location)));
				}
			}

			checkProperties(props);

		} catch (Exception e) {
			log.warn("Configuration file '" + location + "' not loaded.");
		}
	}

	@SuppressWarnings("unchecked")
	public static void checkProperties(Properties properties) {

		for (Entry<Object, Object> entry : properties.entrySet()) {

			// verify that all registered properties have the right type
			// (others might have not been registered because some module is not
			// in use)

			String key = (String) entry.getKey();
			String value = (String) entry.getValue();

			for (Property<Object, Object> prop : (Collection<Property<Object, Object>>) (Object) ScrayProperties
					.getRegisteredProperties()) {

				if (prop.getName().equals(key)) {

					if (!prop.checkConstraints(prop.fromString(value))) {

						throw new RuntimeException(
								new PropertyConstraintViolatedException(key));
					}
				}
			}
		}
	}

	public static String DEFAULT_PROPERTY_FILE = "scray.properties";
	public static String DEFAULT_JVM_ARGUMENT = "scray-properties";
	
	public String toString() {
		return "PropertyFileStorage, location='"+ location + "', size=" + props.size() + ", type=" + fileLocationType;
	}
}
