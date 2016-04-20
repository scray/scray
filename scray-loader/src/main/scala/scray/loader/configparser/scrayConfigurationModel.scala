package scray.loader.configparser

import scray.loader.configuration.DBMSConfigProperties
import com.twitter.util.Duration
import scray.querying.description.TableIdentifier
import scray.loader.configuration.QueryspaceIndexstore

/**
 * the whole configuration is ScrayConfiguration
 */
case class ScrayConfiguration(stores: Seq[DBMSConfigProperties],
    urls: Seq[ScrayQueryspaceConfigurationURL])

/**
 * Where a bunch of config-files for queryspaces reside and how often to reload these.
 */
case class ScrayQueryspaceConfigurationURL(url: String, reload: ScrayQueryspaceConfigurationURLReload)
    
/**
 * How often a queryspace configuration file need to be reloaded.
 * Default will be 120 seconds. None means that no reloading will
 * be performed.
 */
case class ScrayQueryspaceConfigurationURLReload(seconds: Option[Int] = Some(ScrayQueryspaceConfigurationURLReload.DEFAULT_URL_RELOAD)) {
  def isEmpty = seconds.isEmpty
  def isNever = isEmpty
  def getDuration: Duration = seconds.map { secs => Duration.fromSeconds(secs) }.getOrElse(Duration.Top)
}
object ScrayQueryspaceConfigurationURLReload {
  val DEFAULT_URL_RELOAD = 120
}

/**
 * 
 */
case class ScrayQueryspaceConfiguration(
    name: String,
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
