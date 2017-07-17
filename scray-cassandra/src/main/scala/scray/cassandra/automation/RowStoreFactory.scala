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
package scray.cassandra.automation

import com.datastax.driver.core.{Row => CassRow}
import com.twitter.util.FuturePool
import com.typesafe.scalalogging.slf4j.LazyLogging
import com.websudos.phantom.CassandraPrimitive
import scray.cassandra.{CassandraQueryableSource, CassandraTableNonexistingException}
import scray.cassandra.extractors.{CassandraExtractor, DomainToCQLQueryMapping}
import scray.cassandra.rows.GenericCassandraRowStoreMapper
import scray.cassandra.sync.CassandraDbSession
import scray.cassandra.util.CassandraUtils
import scray.querying.description.{Column, TableIdentifier}
import scray.querying.queries.DomainQuery
import scray.querying.sync.DbSession

import scala.collection.JavaConverters._




/**
 * a factory for rows stores which reads table Metadata from Cassandra and builds CQLCassandraRowStores
 */
object RowStoreFactory extends LazyLogging {
  
  /**
   * create a row store object using metadata extraction of the datastax java driver
   */
  def getRowStore[Q <: DomainQuery](ti: TableIdentifier, session: DbSession[_, _, _])(
                  implicit typeMap: Map[String, CassandraPrimitive[_]], futurePool: FuturePool): 
                    (Option[CassandraQueryableSource[Q]], List[(String, CassandraPrimitive[_])]) = {
    // retrieve table metadata
    session match {
      case cassSession: CassandraDbSession => 
        val cassExtractor = new CassandraExtractor[Q](cassSession.cassandraSession, ti, futurePool)
        val tableMetaOpt = Option(CassandraUtils.getTableMetadata(ti, cassSession.cassandraSession))
        tableMetaOpt.map { tableMeta => 
          logger.debug(s"Fetching column information for ${tableMeta.getName}")
          val metaInfo = tableMeta.getColumns().asScala.map(colMeta => 
            (colMeta.getName(), typeMap.get(colMeta.getType().getName().toString()).get)).toList
          val allColumns = cassExtractor.getColumns
          val rowKeys = cassExtractor.getRowKeyColumns
          val clusterKeys = cassExtractor.getClusteringKeyColumns
          (Some(new CassandraQueryableSource(ti,
            rowKeys, 
            clusterKeys,
            allColumns,
            allColumns.map(col => 
              // TODO: ManualIndexConfiguration and Map of Splitter must be extracted from config
              cassExtractor.getColumnConfiguration(cassSession, ti.dbId, ti.tableId, Column(col.columnName, ti), None, Map())),
            cassSession.cassandraSession,
            new DomainToCQLQueryMapping[Q, CassandraQueryableSource[Q]](),
            futurePool,
            GenericCassandraRowStoreMapper.cassandraRowToScrayRowMapper(ti))),
          metaInfo)
        }.getOrElse {
          throw new CassandraTableNonexistingException(ti.toString())
        }
      case _ => throw new RuntimeException("Row-Store generator has been called without a Cassandra Session. This is a bug. Please report.")
    }
  }
}
