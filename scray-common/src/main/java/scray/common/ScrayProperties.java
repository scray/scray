package scray.common;

import java.io.IOException;
import java.util.Properties;

public class ScrayProperties {
	private static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(ScrayProperties.class);

	public static final String LOCATION = "scray.properties";

	public static final String RESULT_COMPRESSION_MIN_SIZE_NAME = "RESULT_COMPRESSION_MIN_SIZE";
	public static final int RESULT_COMPRESSION_MIN_SIZE_VALUE = 1024;

	public static Properties props;

	static {
		props = new Properties();
		try {
			props.load(ScrayProperties.class.getClassLoader()
					.getResourceAsStream(LOCATION));
		} catch (IOException e) {
			log.warn("Failed to load scray.properties.", e);
		}
	}
}
