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

import com.datastax.driver.core.{ KeyspaceMetadata, Metadata, TableMetadata, ResultSet, Session }
import org.yaml.snakeyaml.Yaml
import com.twitter.util.Try
import java.util.{ Map => JMap, HashMap => JHashMap }
import scray.querying.description.TableIdentifier

object CassandraUtils {

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getKeyspaceMetadata(session: Session, keyspace: String): KeyspaceMetadata =
    session.getCluster().getMetadata().getKeyspace(Metadata.quote(keyspace))

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getTableMetadata(cf: String, km: KeyspaceMetadata): TableMetadata = {
    km.getTable(Metadata.quote(cf))
  }

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getTableMetadata(ti: TableIdentifier, session: Session, km: Option[KeyspaceMetadata] = None): TableMetadata = {
    val kspaceMeta = km match {
      case Some(ksm) => ksm
      case None      => getKeyspaceMetadata(session, ti.dbId)
    }
    kspaceMeta.getTable(Metadata.quote(ti.tableId))
  }

  /**
   * *not so fast* and *not thread-safe* method to write a property into the table comment of Cassandra.
   * If s.th. else is in the table comment, it will be overwritten.
   * Uses YAML to store properties in a string.
   * If you need synchronization please use external synchronization, e.g. Zookeeper.
   */
  def writeTablePropertyToCassandra(ti: TableIdentifier, session: Session, property: String, value: String): ResultSet = {
    val yaml = createYaml(property, value, getTablePropertiesFromCassandra(ti, session))
    val cql = s"ALTER TABLE ${ti.dbId}.${ti.tableId} WITH comment='$yaml'"
    session.execute(cql)
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
  def getTablePropertiesFromCassandra(ti: TableIdentifier, session: Session): Option[JMap[String, String]] = {
    val tableMeta = getTableMetadata(ti, session)
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
  def getTablePropertyFromCassandra(ti: TableIdentifier, session: Session, property: String): Option[String] =
    getTablePropertiesFromCassandra(ti, session).flatMap(map => Option(map.get(property)))
}
