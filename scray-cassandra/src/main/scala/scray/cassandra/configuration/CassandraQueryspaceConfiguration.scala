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
package scray.cassandra.configuration

import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreSession
import scray.querying.Query
import scray.querying.Registry
import scray.querying.description.ColumnConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.cassandra.extractors.CassandraExtractor
import scray.querying.description.Row
import scray.querying.description.Column
import scray.querying.source.indexing.IndexConfig
import scray.querying.description.TableConfiguration

/**
 * configuration of a simple Cassandra-based query space.
 * If all tables used are based on Cassandra this class can be used out-of-the-box.
 * 
 * name is used to identify the new query space
 * tables is a list of Storehaus-Cassandra-stores you can use to query anything
 * indexes is a concrete list of things which are manually indexed
 * (auto-indexed columns are automatically detected for Cassandra)  
 */
class CassandraQueryspaceConfiguration(
    override val name: String,
    val tables: Set[(AbstractCQLCassandraStore[_, _], ((_) => Row, Option[String]))],
    // mapping from indexed table and the indexed column to the table containing the index and the ref column
    val indexes: Map[(AbstractCQLCassandraStore[_, _], String),
      (AbstractCQLCassandraStore[_, _], String, IndexConfig, Option[Function1[_,_]])]
) extends QueryspaceConfiguration(name) {

  lazy val tableRowMapperMap: Map[AbstractCQLCassandraStore[_, _], ((_) => Row, Option[String])] = tables.toMap
  
  override def queryCanBeOrdered(query: Query): Option[ColumnConfiguration] = {
    // TODO: maybe this is right for some cases, but we must implement all:
    //   - what happens if the manual index is not going to be used
    //   - what happens if the main query doesn't use the table with the clustering column 
    query.getOrdering.flatMap { ordering =>
      Registry.getQuerySpaceColumn(query.getQueryspace, ordering.column).flatMap { colConfig =>
        colConfig.index.flatMap(index => if(index.isManuallyIndexed.isDefined && index.isSorted) {
          Some(colConfig)
        } else {
          None
        }).orElse {
          Registry.getQuerySpaceTable(query.getQueryspace, ordering.column.table).flatMap { table =>
            if(table.clusteringKeyColumns.size > 0 && table.clusteringKeyColumns(0) == ordering.column) {
              Some(colConfig)
            } else {
              None
            }
          }
        }
      }
    }
  }
  
  override def queryCanBeGrouped(query: Query): Option[ColumnConfiguration] = queryCanBeOrdered(query)
  
  override def getColumns: List[ColumnConfiguration] = tables.toList.flatMap ( table => {
    // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
    val typeReducedTable = table._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]]
    val extractor = CassandraExtractor.getExtractor(typeReducedTable, table._2._2)
    val allColumns = extractor.getTableConfiguration(table._2._1).allColumns
    allColumns.map { col =>
      val index = extractor.createManualIndexConfiguration(col, typeReducedTable, indexes, tableRowMapperMap)
      extractor.getColumnConfiguration(typeReducedTable, col, this, index)
    }
  })
  
  override def getTables: Set[TableConfiguration[_, _, _]] = tables.map ( table => {
    // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
    val typeReducedTable = table._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]]
    val extractor = CassandraExtractor.getExtractor(typeReducedTable, table._2._2)
    extractor.getTableConfiguration(table._2._1)
  })
}
