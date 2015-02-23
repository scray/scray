package scray.common.properties.predefined;

import scray.common.properties.IntProperty;
import scray.common.properties.SocketListProperty;
import scray.common.properties.StringProperty;

public class PredefinedProperties {
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
	public final static StringProperty CASSANDRA_QUERY_CLUSTER_NAME = new StringProperty(
			"CASSANDRA_QUERY_CLUSTER_NAME", "Query Cluster");
	public final static StringProperty CASSANDRA_INDEX_CLUSTER_NAME = new StringProperty(
			"CASSANDRA_INDEX_CLUSTER_NAME", "Index Cluster");
	public final static IntProperty MINUTES_PER_BATCH = new IntProperty(
			"MINUTES_PER_BATCH", 1);

	/* Following properties used as constants (defaults) */

	public final static StringProperty INDEXING_HDFS_PROPERTIES_KEY = new StringProperty(
			"INDEXING_HDFS_PROPERTIES_KEY", "indexing.hdfs.properties");
	public final static StringProperty INDEXING_HDFS_PROPERTIES_PATH = new StringProperty(
			"INDEXING_HDFS_PROPERTIES_PATH", "/tmp/bdq-indexing.properties");

}
