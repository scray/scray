package scray.common.properties.predefined;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

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
	public final static StringProperty CASSANDRA_QUERY_CLUSTER_DC = new StringProperty(
			"CASSANDRA_QUERY_CLUSTER_DC", "DC1");
	public final static StringProperty CASSANDRA_INDEX_CLUSTER_NAME = new StringProperty(
			"CASSANDRA_INDEX_CLUSTER_NAME", "Index Cluster");
	public final static StringProperty CASSANDRA_INDEX_CLUSTER_DC = new StringProperty(
			"CASSANDRA_INDEX_CLUSTER_DC", "DC2");
	public final static IntProperty MINUTES_PER_BATCH = new IntProperty(
			"MINUTES_PER_BATCH", 30);

	private static Set<InetSocketAddress> defaultScrayServiceIps = new HashSet<InetSocketAddress>();

	static {
		defaultScrayServiceIps.add(new InetSocketAddress("0.0.0.0", 18181));
	}

	public final static SocketListProperty SCRAY_SERVICE_IPS = new SocketListProperty(
			"SCRAY_SERVICE_IPS", 18181, defaultScrayServiceIps);
	public final static SocketListProperty SCRAY_MEMCACHED_IPS = new SocketListProperty(
			"SCRAY_MEMCACHED_IPS", 11211);

}
