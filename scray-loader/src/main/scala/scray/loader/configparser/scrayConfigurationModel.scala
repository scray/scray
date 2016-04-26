package scray.loader.configparser

import com.twitter.util.Duration
import java.net.{ InetAddress, InetSocketAddress }
import java.util.{ Set => JSet }
import scray.common.properties.{ ScrayProperties, SocketListProperty }
import scray.common.properties.predefined.PredefinedProperties
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.configuration.DBMSConfigProperties
import scray.querying.Registry
import scray.querying.planning.PostPlanningActions
import scray.querying.description.TableIdentifier
import scray.loader.configuration.QueryspaceIndexstore

/**
 * the whole configuration is ScrayConfiguration
 */
case class ScrayConfiguration(
    service: ScrayServiceOptions,
    stores: Seq[_ <: DBMSConfigProperties],
    urls: Seq[ScrayQueryspaceConfigurationURL])

/**
 * Options to be set on the service
 */
case class ScrayServiceOptions(seeds: Set[InetAddress] = Set(),
    advertiseip: InetAddress, 
    serviceIp: InetAddress = InetAddress.getByName(PredefinedProperties.SCRAY_SERVICE_LISTENING_ADDRESS.getDefault),
    compressionsize: Int = 1024,
    memcacheips: Set[InetSocketAddress] = Set(), 
    serviceport: Int = PredefinedProperties.SCRAY_QUERY_PORT.getDefault,
    metaport: Int = PredefinedProperties.SCRAY_META_PORT.getDefault,
    lifetime: Duration = ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME.getDefault(),
    writeDot: Boolean = false) {
  def propagate: Unit = {
    import scala.collection.convert.decorateAsJava._
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_QUERY_PORT, new Integer(serviceport), true)
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_META_PORT, new Integer(metaport), true)
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_MEMCACHED_IPS.getName, memcacheips.asJava, true)
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_SERVICE_LISTENING_ADDRESS, serviceIp.getHostAddress, true)
    ScrayProperties.setPropertyValue(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getName, compressionsize, true)
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_SERVICE_HOST_ADDRESS.getName, advertiseip, true)
    ScrayProperties.setPropertyValue(ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME.getName, lifetime, true)
    ScrayProperties.setPropertyValue(PredefinedProperties.SCRAY_SEED_IPS.getName, 
        seeds.map(host => new InetSocketAddress(host, metaport)).asJava, true)
    Registry.queryPostProcessor = if(writeDot) PostPlanningActions.writeDot else PostPlanningActions.doNothing
  }
  def memoryMap: Map[String, _] = {
    import scala.collection.convert.decorateAsJava._
    Registry.queryPostProcessor = if(writeDot) PostPlanningActions.writeDot else PostPlanningActions.doNothing    
    Map((PredefinedProperties.SCRAY_QUERY_PORT.getName, new Integer(serviceport)),
        (PredefinedProperties.SCRAY_META_PORT.getName, new Integer(metaport)),
        (PredefinedProperties.SCRAY_MEMCACHED_IPS.getName, memcacheips.asJava),
        (PredefinedProperties.SCRAY_SERVICE_LISTENING_ADDRESS.getName, serviceIp.getHostAddress),
        (PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getName, compressionsize),
        (PredefinedProperties.SCRAY_SERVICE_HOST_ADDRESS.getName, advertiseip.getHostAddress),
        (ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME.getName, lifetime),
        (PredefinedProperties.SCRAY_SEED_IPS.getName, seeds.map(host => new InetSocketAddress(host, metaport)).asJava))
  }
}

    
/**
 * Where a bunch of config-files for queryspaces reside and how often to reload these.
 */
case class ScrayQueryspaceConfigurationURL(url: String, reload: ScrayQueryspaceConfigurationURLReload)
    
/**
 * How often a queryspace configuration file need to be reloaded.
 * Default will be 120 seconds. None means that no reloading will
 * be performed.
 */
case class ScrayQueryspaceConfigurationURLReload(duration: Option[Duration] = Some(ScrayQueryspaceConfigurationURLReload.DEFAULT_URL_RELOAD)) {
  def isEmpty = duration.isEmpty
  def isNever = isEmpty
  def getDuration: Duration = duration.getOrElse(Duration.Top)
}
object ScrayQueryspaceConfigurationURLReload {
  val DEFAULT_URL_RELOAD = Duration.fromSeconds(120)
}

/**
 * Configuration object representing a single queryspace
 */
case class ScrayQueryspaceConfiguration(
    name: String,
    version: Long,
    syncTable: Option[TableIdentifier],
    rowStores: Seq[TableIdentifier],
    indexStores: Seq[QueryspaceIndexstore]/*,
    materializedViews: Seq[ScrayMaterializedView]*/)

/**
 * sub-parameters if the store is versioned; i.e. all we need for 
 * 
 */
case class ScrayVersionedStore()

case class ScrayMaterializedView()

case class ScannedQueryspaceConfigfiles(path: String, name: String, version: Long, queryspaceConfig: ScrayQueryspaceConfiguration)

/**
 * represents a list of configured users, authentications and queryspaces
 */
case class ScrayUsersConfiguration(users: Seq[ScrayAuthConfiguration])

/**
 * represents a single configured user, authentication and queryspaces
 */
case class ScrayAuthConfiguration(user: String, pwd: String, method: ScrayAuthMethod.Value, queryspaces: Set[String])

/** 
 * supported authentication directories
 */
object ScrayAuthMethod extends Enumeration {
  val Plain, LDAP = Value
}

