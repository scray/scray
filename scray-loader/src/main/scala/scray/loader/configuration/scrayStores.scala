package scray.loader.configuration

import scala.collection.mutable.{ ArrayBuffer, HashMap }
import scray.loader.configparser.{ ConfigProperties, ReadableConfig, ScrayConfiguration, UpdatetableConfiguration }
import scray.querying.sync.types.DbSession
import scray.loader.DBMSUndefinedException
import scray.cassandra.automation.CassandraSessionHandler
import scray.cassandra.extractors.CassandraExtractor
import scray.cassandra.automation.CassandraStoreGenerators
import scray.cassandra.rows.GenericCassandraRowStoreMapper
import scray.cassandra.automation.CassandraSessionHandler
import scray.querying.storeabstraction.StoreGenerators
import scray.cassandra.automation.CassandraStoreGenerators
import scray.cassandra.rows.GenericCassandraRowStoreMapper

/**
 * abstraction for the management of configuration of stores
 */
class ScrayStores(startConfig: ScrayConfiguration) {
  import GenericCassandraRowStoreMapper.cassandraPrimitiveTypeMap
  
  type SessionChangeListener = (String, DbSession[_, _, _]) => Unit
  
  private val storeConfigs: HashMap[String, DBMSConfiguration[_ <: DBMSConfigProperties]] = new HashMap[String, DBMSConfiguration[_ <: DBMSConfigProperties]]
  private val storeSessions: HashMap[String, DbSession[_, _, _]] = new HashMap[String, DbSession[_, _, _]]
  private val sessionChangeListeners: ArrayBuffer[SessionChangeListener] = new ArrayBuffer[SessionChangeListener]
  
  def getSessionForStore(dbmsId: String): Option[DbSession[_, _, _]] = storeSessions.get(dbmsId)
  
  def addSessionChangeListener(listener: SessionChangeListener): Unit = sessionChangeListeners += listener
  
  def updateStoreConfigs(configUpdate: ScrayConfiguration): Unit = { 
    val newConf = configUpdate.stores.map { storeprops =>
      val current = storeConfigs.get(storeprops.getName)
      val dbmsconfig = current match {
        case Some(entry) => entry
        case None => createDBMSConfigurationForProperties(storeprops)
      }
      dbmsconfig.updateConfiguration(configUpdate).map { session =>
        sessionChangeListeners.foreach { _(storeprops.getName, session) }
      }
      (storeprops.getName, dbmsconfig)
    }
    storeConfigs.clear()
    storeConfigs ++= (newConf)
  }
 
  /**
   * factory-method for store configurations from resp. properties
   */
  def createDBMSConfigurationForProperties(properties: DBMSConfigProperties): DBMSConfiguration[_ <: DBMSConfigProperties] = {
    properties match {
      case cass: CassandraClusterProperties => new CassandraClusterConfiguration(cass)
      case jdbc: JDBCProperties => new JDBCConfiguration(jdbc)
    }
  }
  
  def updateConfiguration(configUpdate: ScrayConfiguration) = updateStoreConfigs(configUpdate)

  // last thing after initializing vals is starting up...
  updateConfiguration(startConfig)

  val cassandraSessionHandler = new CassandraSessionHandler
  
  def getStoreGenerator(dbId: String, session: DbSession[_, _, _], queryspace: String): StoreGenerators = { 
    storeConfigs.get(dbId).map { config => config match {
      case cassConfig: CassandraClusterConfiguration =>
        new CassandraStoreGenerators(dbId, session, cassandraSessionHandler)
      case _ => throw new DBMSUndefinedException(dbId, queryspace)
    }}.getOrElse(throw new DBMSUndefinedException(dbId, queryspace))
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
  
  override def updateConfiguration(configUpdate: ScrayConfiguration): Option[DbSession[_, _, _]] = {
    if(config.isEmpty) {
      // TODO: re-read in case of previously erasing the config -> i.e. probably we need to make it new...
      throw new UnsupportedOperationException("re-reading the config is not supported in case of previously erasing it")      
    }
    val oldConfig = config.get
    val newConfig = readConfig(configUpdate, oldConfig)
    if(oldConfig.needsUpdate(newConfig)) {
      config = newConfig
      performUpdateTasks()
      Some(getSession)
    } else {
      None
    }
  }
  
  def getSession: DbSession[_, _, _]
}
