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
package scray.cassandra.util

import com.datastax.driver.core.{ KeyspaceMetadata, Metadata, TableMetadata }
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import org.yaml.snakeyaml.Yaml
import com.twitter.util.Try
import java.util.{ Map => JMap, HashMap => JHashMap }
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session

object CassandraUtils {

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getKeyspaceMetadata(cf: StoreColumnFamily): KeyspaceMetadata =
    cf.session.getSession.getCluster().getMetadata().getKeyspace(Metadata.quote(cf.session.getKeyspacename))

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getKeyspaceMetadata(session: Session, keyspace: String): KeyspaceMetadata =
    session.getCluster().getMetadata().getKeyspace(Metadata.quote(keyspace))

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getTableMetadata(cf: String, km: KeyspaceMetadata): TableMetadata = {
    km.getTable(cf)
  }

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getTableMetadata(cf: StoreColumnFamily, km: Option[KeyspaceMetadata] = None): TableMetadata = {
    val kspaceMeta = km match {
      case Some(ksm) => ksm
      case None      => getKeyspaceMetadata(cf)
    }
    kspaceMeta.getTable(cf.getPreparedNamed)
  }

  /**
   * *not so fast* and *not thread-safe* method to write a property into the table comment of Cassandra.
   * If s.th. else is in the table comment, it will be overwritten.
   * Uses YAML to store properties in a string.
   * If you need synchronization please use external synchronization, e.g. Zookeeper.
   */
  def writeTablePropertyToCassandra(cf: StoreColumnFamily, property: String, value: String): ResultSet = {
    val yaml = createYaml(property, value, getTablePropertiesFromCassandra(cf))
    val cql = s"ALTER TABLE ${cf.getPreparedNamed} WITH comment='$yaml'"
    cf.session.getSession.execute(cql)
  }

  def createYaml(property: String, value: String, currentMap: Option[JMap[String, String]] = None): String = {
    def escapeQuotes(str: String): String = str.replaceAll("'", "''")
    val map = currentMap match {
      case Some(properties) =>
        properties.put(property, value)
      case None =>
        val properties = new JHashMap[String, String]
        properties.put(property, value)
        properties
    }
    escapeQuotes(new Yaml().dump(map))
  }

  /**
   * *not so fast* method to read all table properties stored in a comment of a column family from Cassandra.
   * Uses YAML to store properties in a string.
   * If you need synchronization to sync with writers please use external synchronization, e.g. Zookeeper.
   */
  def getTablePropertiesFromCassandra(cf: StoreColumnFamily): Option[JMap[String, String]] = {
    val tableMeta = getTableMetadata(cf)
    val currentYaml = Option(tableMeta.getOptions.getComment)
    val yaml = new Yaml()
    currentYaml.flatMap { content =>
      Try {
        Option(yaml.load(content).asInstanceOf[JMap[String, String]])
      }.toOption.flatten
    }
  }

  /**
   * *not so fast* method to read a table property stored in a comment of a column family from Cassandra.
   * Uses YAML to store properties in a string.
   * If you need synchronization to sync with writers please use external synchronization, e.g. Zookeeper.
   */
  def getTablePropertyFromCassandra(cf: StoreColumnFamily, property: String): Option[String] =
    getTablePropertiesFromCassandra(cf).flatMap(map => Option(map.get(property)))
}
