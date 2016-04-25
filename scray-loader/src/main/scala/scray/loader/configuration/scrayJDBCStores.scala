package scray.loader.configuration

import com.twitter.util.Try
import scala.util.{Try => STry}
import scray.common.properties.predefined.PredefinedProperties
import scray.common.properties.ScrayProperties
import com.datastax.driver.core.policies.Policies
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import scray.common.tools.ScrayCredentials
import scray.loader.configparser.ReadableConfig
import scala.collection.convert.decorateAsScala.asScalaSetConverter
import scray.loader.configparser.ScrayConfiguration
import scray.querying.sync.types.DbSession

/**
 * JDBC properties, needed to setup a JDBC connection
 */
case class JDBCProperties(url: String,
      credentials: ScrayCredentials = new ScrayCredentials(null, null),
      name: Option[String] = None) extends DBMSConfigProperties {
  override def getName: String = name.getOrElse("jdbc")
  override def setName(newName: Option[String]): DBMSConfigProperties = this.copy(name = newName)
}

trait JDBCProperty
case class JDBCURLProperty(url: String) extends JDBCProperty
case class JDBCCredentialsProperty(credentials: ScrayCredentials) extends JDBCProperty

/**
 * sets up and manages a Cassandra Cluster
 */
class JDBCConfiguration(override protected val startconfig: JDBCProperties) 
    extends DBMSConfiguration[JDBCProperties](startconfig) {

  var currentURL: Option[String] = None
  
  override def performUpdateTasks(): Unit = {
    // TODO: examine what tasks need to be done for JDBC...
  }

  override def getSession: DbSession[_, _, _] = {
    // TODO: next line is bullshit
    new DbSession[Int, String, String]("") {
      override def execute(statement: Int): STry[String] = STry("") 
      override def execute(statement: String): STry[String] = STry("")
      override def insert(statement: String): STry[String] = STry("")
    }
  } 

  override def readConfig(config: ScrayConfiguration, old: JDBCProperties): Option[JDBCProperties] = 
    JDBCConfiguration.readConfig(config, old)
}

object JDBCConfiguration extends ReadableConfig[JDBCProperties] {
  
  override def readConfig(config: ScrayConfiguration, old: JDBCProperties): Option[JDBCProperties] = 
    config.stores.find { storecf => storecf.getName == old.getName }.flatMap { 
      case jdbc: JDBCProperties => Some(jdbc)
      case _ => None
    }
}