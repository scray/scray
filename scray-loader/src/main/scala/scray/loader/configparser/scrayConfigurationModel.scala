// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.loader.configparser

import com.twitter.util.Duration

import java.net.{ InetAddress, InetSocketAddress }

import scala.collection.convert.decorateAsJava
import scala.collection.convert.decorateAsJava.setAsJavaSetConverter

import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties
import scray.core.service.properties.ScrayServicePropertiesRegistrar
import scray.loader.configuration.QueryspaceIndexstore
import scray.querying.Registry
import scray.querying.description.TableIdentifier
import scray.querying.planning.PostPlanningActions
import scray.loader.configuration.DBMSConfigProperties
import scray.loader.configuration.QueryspaceMaterializedView

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
    compressionsize: Int = PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE.getDefault,
    memcacheips: Set[InetSocketAddress] = Set(), 
    serviceport: Int = PredefinedProperties.SCRAY_QUERY_PORT.getDefault,
    metaport: Int = PredefinedProperties.SCRAY_META_PORT.getDefault,
    lifetime: Duration = ScrayServicePropertiesRegistrar.SCRAY_ENDPOINT_LIFETIME.getDefault,
    writeDot: Boolean = false,
    interval: Int = 3600) {
  def propagate: Unit = {
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
  def isEmpty: Boolean = duration.isEmpty
  def isNever: Boolean = isEmpty
  def getDuration: Duration = duration.getOrElse(Duration.Top)
}
object ScrayQueryspaceConfigurationURLReload {
  // scalastyle:off magic.number
  val DEFAULT_URL_RELOAD = Duration.fromSeconds(120)
  // scalastyle:on magic.number
}

/**
 * Configuration object representing a single queryspace
 */
case class ScrayQueryspaceConfiguration(
    name: String,
    version: Long,
    syncTable: Option[TableIdentifier],
    rowStores: Seq[TableIdentifier],
    indexStores: Seq[QueryspaceIndexstore],
    materializedViews: Seq[QueryspaceMaterializedView]
    )

/**
 * sub-parameters if the store is versioned; i.e. all we need for 
 * 
 */
case class ScrayVersionedStore()

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

