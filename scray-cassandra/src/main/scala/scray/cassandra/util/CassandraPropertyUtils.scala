package scray.cassandra.util

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreHost
import scray.common.properties.ScrayProperties
import scala.collection.JavaConverters.asScalaSetConverter
import java.net.InetSocketAddress
import scray.common.properties.Property
import scray.common.properties.ScrayProperties.Phase
import scray.common.properties.IntProperty

/**
 * utility functions for configurable Cassandra properties
 */
object CassandraPropertyUtils {

  def getCassandraHostProperty() : Set[StoreHost] = ScrayProperties.getPropertyValue(ScrayProperties.CASSANDRA_QUERY_SEED_IPS).
    asScala.toSet[InetSocketAddress].map(inetsocket => StoreHost(s"${inetsocket.getHostString}:${inetsocket.getPort}"))

  def performDefaultPropertySystemInitialization(additionPropertiesToRegister : Set[Property[_, _]] = Set()) : Unit = {
    ScrayProperties.registerProperty(ScrayProperties.CASSANDRA_QUERY_SEED_IPS)
    ScrayProperties.registerProperty(ScrayProperties.RESULT_COMPRESSION_MIN_SIZE)
    additionPropertiesToRegister.foreach(ScrayProperties.registerProperty(_))
    ScrayProperties.setPhase(Phase.config)
    ScrayProperties.setPhase(Phase.use)
  }
}