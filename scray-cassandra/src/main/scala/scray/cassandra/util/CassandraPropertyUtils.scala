package scray.cassandra.util

import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreHost
import scray.common.properties.ScrayProperties
import scala.collection.JavaConverters.asScalaSetConverter
import java.net.InetSocketAddress
import scray.common.properties.Property
import scray.common.properties.ScrayProperties.Phase
import scray.common.properties.IntProperty
import scray.common.properties.ScrayPropertyRegistration
import scray.common.properties.predefined.CommonCassandraRegistration
import scray.common.properties.predefined.CommonCassandraLoader
import scray.common.properties.predefined.PredefinedProperties

/**
 * utility functions for configurable Cassandra properties
 */
object CassandraPropertyUtils {

  def getCassandraHostProperty() : Set[StoreHost] = ScrayProperties.getPropertyValue(PredefinedProperties.CASSANDRA_QUERY_SEED_IPS).
    asScala.toSet[InetSocketAddress].map(inetsocket => StoreHost(s"${inetsocket.getHostString}:${inetsocket.getPort}"))

  def performDefaultPropertySystemInitialization(additionPropertiesToRegister : Set[Property[_, _]] = Set()) : Unit = {
    additionPropertiesToRegister.foreach(ScrayProperties.registerProperty(_))
    ScrayPropertyRegistration.addRegistrar(new CommonCassandraRegistration)
    ScrayPropertyRegistration.addLoader(new CommonCassandraLoader)
    ScrayPropertyRegistration.performPropertySetup
    
  }
}