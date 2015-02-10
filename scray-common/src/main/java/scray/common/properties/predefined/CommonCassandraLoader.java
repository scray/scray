package scray.common.properties.predefined;

import scray.common.properties.PropertyException;
import scray.common.properties.PropertyFileStorage;
import scray.common.properties.ScrayProperties;
import scray.common.properties.ScrayPropertyRegistration;

public class CommonCassandraLoader extends ScrayPropertyRegistration.PropertyLoaderImpl {

	public CommonCassandraLoader() {
		super("SIL-Stores-Property-Config");
	}
	
	@Override
	public void load() throws PropertyException {
	    ScrayProperties.addPropertiesStore(
	      new PropertyFileStorage("bdq-sil-stores.properties", PropertyFileStorage.FileLocationTypes.JarPropertiesFile));
	    ScrayProperties.addPropertiesStore(
	      new PropertyFileStorage("bdq-sil-stores-properties", PropertyFileStorage.FileLocationTypes.LocalPropertiesFile));
	}
}
