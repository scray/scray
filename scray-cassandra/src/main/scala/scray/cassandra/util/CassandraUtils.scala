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
import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.DBColumnImplementation
import java.util.{ Iterator => JIterator }
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import java.util.ArrayList
import scray.querying.sync.DbSession
import scala.collection.mutable.ArrayBuffer
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scala.collection.mutable.ListBuffer
import scray.querying.sync.JobInfo
import com.datastax.driver.core.BatchStatement
import scray.querying.sync.State.State
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions
import scray.querying.sync.SyncTable
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.StatementExecutionError
import scala.util.Failure
import scala.util.Success
import scray.querying.description.TableIdentifier
import scala.collection.mutable.HashSet
import scray.querying.sync.AbstractRow
import scray.querying.sync.ColumnWithValue
import scray.querying.sync.VoidTable
import scray.querying.sync.RowWithValue
import scray.querying.sync.Table
import scray.querying.sync.State
import scray.querying.sync.AbstractTypeDetection
import scray.querying.sync.DBTypeImplicit
import com.datastax.driver.core.BatchStatement
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.websudos.phantom.CassandraPrimitive
import java.util.{ Iterator => JIterator }
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.util.Failure
import scala.util.Success
import scray.querying.description.TableIdentifier
import scray.querying.sync.JobInfo
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.OnlineBatchSync
import scray.querying.sync.OnlineBatchSyncWithTableIdentifier
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.StateMonitoringApi
import scray.querying.sync.StatementExecutionError
import java.util.{ Iterator => JIterator }
import scray.querying.sync.JobLockTable
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.SyncTableBasicClasses

object CassandraUtils extends LazyLogging with Serializable {

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
    
        def getNewestRow(rows: java.util.Iterator[Row], columnName: String): Option[Row] = {
    import scala.math.Ordering._
    val comp = implicitly[Ordering[Long]]
    getComptRow(rows, comp.gt, columnName)
  }

  def getOldestRow(rows: java.util.Iterator[Row], columnName: String): Option[Row] = {
    import scala.math.Ordering._
    val comp = implicitly[Ordering[Long]]
    getComptRow(rows, comp.lt, columnName)
  }

  def getComptRow(rows: JIterator[Row], comp: (Long, Long) => Boolean, columnName: String): Option[Row] = {
    @tailrec def accNewestRow(prevRow: Row, nextRows: JIterator[Row]): Row = {
      if (nextRows.hasNext) {
        val localRow = nextRows.next()
        logger.debug(s"Work with row ${prevRow} and ${localRow}")
        val max = if (comp(prevRow.getLong(columnName), localRow.getLong(columnName))) {
          prevRow
        } else {
          localRow
        }
        accNewestRow(max, nextRows)
      } else {
        logger.debug(s"Return newest row ${prevRow}")
        prevRow
      }
    }

    if (rows.hasNext()) {
      Some(accNewestRow(rows.next(), rows))
    } else {
      None
    }
  }
  

  def createTableStatement[T <: AbstractRow](table: Table[T]): Option[String] = {
    val createStatement = s"CREATE TABLE IF NOT EXISTS ${table.keySpace + "." + table.tableName} (" +
      s"${table.columns.foldLeft("")((acc, next) => { acc + next.name + " " + next.getDBType + ", " })} " +
      s"PRIMARY KEY ${table.columns.primaryKey})"
    logger.debug(s"Create table String: ${createStatement} ")
    Some(createStatement)
  }

  def createKeyspaceCreationStatement[T <: AbstractRow](table: Table[T]): Option[String] = {
    Some(s"CREATE KEYSPACE IF NOT EXISTS ${table.keySpace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
      }
}
