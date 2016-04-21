package scray.common.properties.predefined;

import java.util.Arrays;

import scray.common.properties.ConsistencyLevelProperty;
import scray.common.properties.CredentialsProperty;
import scray.common.properties.IntProperty;
import scray.common.properties.SocketListProperty;
import scray.common.properties.StringListProperty;
import scray.common.properties.StringProperty;
import scray.common.tools.ScrayCredentials;

public class PredefinedProperties {

	public final static StringListProperty CONFIGURED_STORES = new StringListProperty(
			"CONFIGURED_STORES", Arrays.asList(new String[] { "cassandra" }));
	public final static IntProperty RESULT_COMPRESSION_MIN_SIZE = new IntProperty(
			"RESULT_COMPRESSION_MIN_SIZE", 1024);
	public final static SocketListProperty CASSANDRA_QUERY_SEED_IPS = new SocketListProperty(
			"CASSANDRA_QUERY_SEED_IPS", 9042);
	public final static SocketListProperty CASSANDRA_INDEX_SEED_IPS = new SocketListProperty(
			"CASSANDRA_INDEX_SEED_IPS", 9042);
	public final static StringProperty CASSANDRA_QUERY_KEYSPACE = new StringProperty(
			"CASSANDRA_QUERY_KEYSPACE", "SIL");
	public final static StringProperty CASSANDRA_INDEX_KEYSPACE = new StringProperty(
			"CASSANDRA_INDEX_KEYSPACE", "SILIDX");
	public final static ConsistencyLevelProperty QUERY_READ_CONSISTENCY = new ConsistencyLevelProperty(
			"QUERY_READ_CONSISTENCY",
			ConsistencyLevelProperty.ConsistencyLevel.LOCAL_ONE);
	public final static StringProperty CASSANDRA_QUERY_CLUSTER_NAME = new StringProperty(
			"CASSANDRA_QUERY_CLUSTER_NAME", "Query Cluster");
	public final static StringProperty CASSANDRA_QUERY_CLUSTER_DC = new StringProperty(
			"CASSANDRA_QUERY_CLUSTER_DC", "DC1");
	public final static CredentialsProperty CASSANDRA_QUERY_CLUSTER_CREDENTIALS = new CredentialsProperty(
			"CASSANDRA_QUERY_CLUSTER_CREDENTIALS", new ScrayCredentials(null, null));
	public final static StringProperty CASSANDRA_INDEX_CLUSTER_NAME = new StringProperty(
			"CASSANDRA_INDEX_CLUSTER_NAME", "Index Cluster");
	public final static StringProperty CASSANDRA_INDEX_CLUSTER_DC = new StringProperty(
			"CASSANDRA_INDEX_CLUSTER_DC", "DC2");
	public final static IntProperty MINUTES_PER_BATCH = new IntProperty(
			"MINUTES_PER_BATCH", 30);
	public final static IntProperty INDEX_ROW_SPREAD = new IntProperty(
			"INDEX_ROW_SPREAD", 15);
	public final static StringProperty INDEX_PARALLELIZATION_COLUMN = new StringProperty(
			"INDEX_PARALLELIZATION_COLUMN", "Spread");
	public final static StringProperty SCRAY_SERVICE_HOST_ADDRESS = new StringProperty(
			"SCRAY_SERVICE_HOST_ADDRESS");
	public final static StringProperty SCRAY_SERVICE_LISTENING_ADDRESS = new StringProperty(
			"SCRAY_SERVICE_LISTENING_ADDRESS", "0.0.0.0");
	public final static IntProperty SCRAY_QUERY_PORT = new IntProperty(
			"SCRAY_QUERY_PORT", 18181);
	public final static IntProperty SCRAY_META_PORT = new IntProperty(
			"SCRAY_META_PORT", 18191);
	public final static SocketListProperty SCRAY_SEED_IPS = new SocketListProperty(
			"SCRAY_SEED_IPS", 18191);
	public final static SocketListProperty SCRAY_MEMCACHED_IPS = new SocketListProperty(
			"SCRAY_MEMCACHED_IPS", 11211);

}