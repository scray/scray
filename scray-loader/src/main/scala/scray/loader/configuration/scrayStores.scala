package scray.loader.configuration

import scala.collection.convert.decorateAsScala.asScalaBufferConverter
import scala.collection.mutable.HashMap
import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties
import scray.loader.configparser.ConfigProperties
import scray.loader.configparser.UpdatetableConfiguration
import scray.loader.configparser.ReadableConfig
import scray.loader.configparser.ScrayConfiguration

/**
 * abstraction for the management of configuration of stores
 */
class ScrayStores extends UpdatetableConfiguration {
  
  lazy val configuredStores: Set[String] = ScrayProperties.getPropertyValue(PredefinedProperties.CONFIGURED_STORES).asScala.toSet 
  
  private lazy val storeConfigs: HashMap[String, DBMSConfiguration[_]] = new HashMap[String, DBMSConfiguration[_]]
  
  override def updateConfiguration(configUpdate: ScrayConfiguration) = {
    // TODO: implement configuration updates
    storeConfigs.foreach { _._2.updateConfiguration(configUpdate) }
  }
}

/**
 * Marker trait for Properties which are specific for a type of store
 */
trait DBMSConfigProperties extends ConfigProperties { self => 
  override def needsUpdate(newProperties: Option[ConfigProperties]): Boolean = super.needsUpdate(newProperties)
  def getName: String
  def setName(name: Option[String]): DBMSConfigProperties
}
  
/**
 * DBMSConfiguration is a configuration abstraction for 
 */
abstract class DBMSConfiguration[T <: DBMSConfigProperties](protected val startconfig: T) 
    extends ReadableConfig[T] with UpdatetableConfiguration {
  
  protected var config: Option[T] = Some(startconfig)
  
  def performUpdateTasks(): Unit
  
  override def updateConfiguration(configUpdate: ScrayConfiguration): Unit = {
    config.map { oldConfig => 
      val newConfig = readConfig(configUpdate, oldConfig)
      if(oldConfig.needsUpdate(newConfig)) {
        config = newConfig
        performUpdateTasks()
      }
    }.orElse {
      // TODO: re-read in case of previously erasing he config -> i.e. probably we need to make it new...
      throw new UnsupportedOperationException("re-reading the config is not supported in case of previously erasing it")
    }
  }
}
