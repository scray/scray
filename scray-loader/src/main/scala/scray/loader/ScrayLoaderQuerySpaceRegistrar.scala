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
//import scray.loader.configparser.ScrayConfigurationParser
//import scray.loader.configparser.ScrayConfiguration
//
//class ScrayLoaderQuerySpaceRegistrar(name: String, configuration: ScrayConfiguration) extends LazyLogging {
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
//  def init() = {
//    logger.info("Initializing DBMS configuration...")
//    // parse configuration files
//    val scrayConfiguration = ScrayConfigurationParser.parse(text, true)
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
