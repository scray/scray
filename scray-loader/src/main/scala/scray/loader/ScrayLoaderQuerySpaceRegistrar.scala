//package scray.loader
//
//import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, Policies, TokenAwarePolicy}
//import com.twitter.storehaus.{QueryableStore, ReadableStore} 
//import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.{DEFAULT_SHUTDOWN_TIMEOUT, StoreCluster, StoreCredentials, StoreSession}  
//import com.twitter.util.Try
//import com.typesafe.scalalogging.slf4j.LazyLogging
//import java.util.concurrent.locks.ReentrantLock
//import org.apache.log4j.Level
//import scala.collection.mutable.HashMap
//import scala.collection.convert.decorateAsScala.asScalaBufferConverter
//import scray.cassandra.rows.CassandraGenericIndexRowMappers
//import scray.cassandra.util.CassandraPropertyUtils
//import scray.common.properties.ScrayProperties
//import scray.common.properties.predefined.PredefinedProperties
//import scray.querying.description.TableIdentifier
//
//class ScrayLoaderQuerySpaceRegistrar(name: String) extends LazyLogging {
//  
//  val sessionLock = new ReentrantLock // lock for sessions
//  val sessions = new HashMap[String, StoreSession]
//
//  case class TemporaryTimeIndexInfoCollector(val indexedCfName: TableIdentifier,
//                           val indexedColumnName: String,
//                           val indexStore: CassandraGenericIndexRowMappers.TimeIndexStore,
//                           val indexPrefix: TableIdentifier,
//                           val initialVersion: Option[Long],
//                           val storeFunc: (Long) => (QueryableStore[_, _], ReadableStore[_, _]))
//
//  
//  def log(msg: String, logLevel: Level = Level.INFO) = {
//    val scraymsg = s"Scray-Loader: $msg"
//    logLevel match {
//      case Level.INFO => logger.info(scraymsg)
//      case Level.WARN => logger.warn(scraymsg)
//      case Level.ERROR => logger.error(scraymsg)
//      case _ => logger.debug(scraymsg)
//    }
//    
//  }
//  
//  
//  
//  def init() = {
//    log("Initializing DBMS configuration...")
//    conf
//  }
//  
//}
//
//object ScrayLoaderQuerySpaceRegistrar extends LazyLogging {
//  
//  def apply(name: String) = {
//    new ScrayLoaderQuerySpaceRegistrar(name).init()
//  }
//}