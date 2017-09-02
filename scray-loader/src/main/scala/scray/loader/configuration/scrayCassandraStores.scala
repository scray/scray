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

import scray.cassandra.util.CassandraPropertyUtils
import com.twitter.util.Try
import scray.common.properties.predefined.PredefinedProperties
import scray.common.properties.ScrayProperties
import com.datastax.driver.core.policies.Policies
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy
import com.datastax.driver.core.policies.TokenAwarePolicy
import scray.common.tools.ScrayCredentials
import scray.loader.configparser.ReadableConfig
import scray.cassandra.extractors.CassandraExtractor
import scala.collection.convert.decorateAsScala.asScalaSetConverter
import scray.loader.configparser.ScrayConfiguration
import com.datastax.driver.core.ConsistencyLevel
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.twitter.util.Throw
import scray.querying.sync.DbSession
import scray.cassandra.sync.CassandraDbSession
import com.datastax.driver.core.Cluster
import java.net.InetAddress
import scala.collection.convert.decorateAsJava._

/**
 * Cassandra properties, needed to setup a Cassandra cluster object
 */
case class CassandraClusterProperties(clusterName: String = PredefinedProperties.CASSANDRA_QUERY_CLUSTER_NAME.getDefault,
      credentials: ScrayCredentials = new ScrayCredentials(),
      hosts: Set[String] = Option(PredefinedProperties.CASSANDRA_QUERY_SEED_IPS.getDefault).
        map(_.asScala.map { addr => addr.toString }.toSet).getOrElse(Set()),
      datacenter: String = PredefinedProperties.CASSANDRA_QUERY_CLUSTER_DC.getDefault,
      name: Option[String] = None) extends DBMSConfigProperties {
  override def getName: String = name.getOrElse(CassandraExtractor.DB_ID)
  override def setName(newName: Option[String]): DBMSConfigProperties = this.copy(name = newName)
}

trait CassandraClusterProperty
case class CassandraClusterNameProperty(name: String) extends CassandraClusterProperty
case class CassandraClusterCredentials(credentials: ScrayCredentials) extends CassandraClusterProperty
case class CassandraClusterHosts(hosts: Set[String]) extends CassandraClusterProperty
case class CassandraClusterDatacenter(dc: String) extends CassandraClusterProperty
case class CassandraClusterConsistency(read: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM, 
    write: ConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM) extends CassandraClusterProperty

/**
 * sets up and manages a Cassandra Cluster
 */
class CassandraClusterConfiguration(override protected val startconfig: CassandraClusterProperties) 
    extends DBMSConfiguration[CassandraClusterProperties](startconfig) with LazyLogging {

  var currentCluster: Option[Cluster] = None
  
  /**
   * initialize a Cassandra cluster, if it is selected as an available store
   */
  def getCassandraCluster: Try[Cluster] = Try {
    val clusterName = config.get.clusterName
    val clusterCredentials = config.get.credentials
    val cassandraHost = config.get.hosts.map(h => InetAddress.getByName(h)).asJava
    val builder = Cluster.builder().addContactPoints(cassandraHost).
      //withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(config.get.datacenter))).
      withReconnectionPolicy(Policies.defaultReconnectionPolicy).
      withRetryPolicy(Policies.defaultRetryPolicy)
    Option(clusterCredentials.getUsername).map(creds =>
      builder.withCredentials(clusterCredentials.getUsername, new String(clusterCredentials.getPassword)))
    builder.build
  }
  
  override def performUpdateTasks(): Unit = {
    // shutdown cluster
    currentCluster.map(_.close)
    // create new cluster
    currentCluster = getCassandraCluster.rescue {  
      case e: Exception =>
        logger.error("Could not initialize Cassandra cluster.", e)
        Throw(e)
    }.toOption
  }
  
  override def getSession: DbSession[_, _, _] = {
    val session = getCassandraCluster.map { _.connect() }.get
    new CassandraDbSession(session)
  }
  
  override def readConfig(config: ScrayConfiguration, old: CassandraClusterProperties): Option[CassandraClusterProperties] = {
    CassandraClusterConfiguration.readConfig(config, old)
  }
}

object CassandraClusterConfiguration extends ReadableConfig[CassandraClusterProperties] {
  
  override def readConfig(config: ScrayConfiguration, old: CassandraClusterProperties): Option[CassandraClusterProperties] =
    config.stores.find { storecf => storecf.getName == old.getName }.flatMap { 
      case cass: CassandraClusterProperties => Some(cass)
      case _ => None
    }
}
