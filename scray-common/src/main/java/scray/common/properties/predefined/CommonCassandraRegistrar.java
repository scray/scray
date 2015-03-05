package scray.common.properties.predefined;

import scray.common.properties.PropertyException;
import scray.common.properties.ScrayProperties;
import scray.common.properties.ScrayPropertyRegistration;

public class CommonCassandraRegistrar implements ScrayPropertyRegistration.PropertyRegistrar {

	@Override 
	public void register() throws PropertyException {
		ScrayProperties.registerProperty(PredefinedProperties.CASSANDRA_QUERY_SEED_IPS);
		ScrayProperties.registerProperty(PredefinedProperties.CASSANDRA_QUERY_CLUSTER_NAME);
		ScrayProperties.registerProperty(PredefinedProperties.CASSANDRA_QUERY_CLUSTER_DC);
	    ScrayProperties.registerProperty(PredefinedProperties.CASSANDRA_QUERY_KEYSPACE);
	}
}
