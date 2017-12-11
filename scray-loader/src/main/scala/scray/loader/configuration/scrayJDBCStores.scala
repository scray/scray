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
import scray.querying.sync.DbSession
import scray.jdbc.sync.JDBCDbSession
import com.typesafe.scalalogging.LazyLogging

/**
 * JDBC properties, needed to setup a JDBC connection
 */
case class JDBCProperties(url: String,
      credentials: ScrayCredentials = new ScrayCredentials(),
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
    extends DBMSConfiguration[JDBCProperties](startconfig) with LazyLogging {

  var currentURL: Option[String] = None
  private var sessioncount = 0
  
  override def performUpdateTasks(): Unit = {
    // TODO: examine what tasks need to be done for JDBC...
  }

  override def getSession: DbSession[_, _, _] = {
    // setup Hikari connection pool for this store by creating a JDBC Session
    // need to check how often this is called
    sessioncount += 1
    logger.info(s"Started new JDBC Session, maybe count is ${sessioncount}")
    new JDBCDbSession(startconfig.url, startconfig.credentials.getUsername, new String(startconfig.credentials.getPassword))
    
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
