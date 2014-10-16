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
    val tables: Set[(AbstractCQLCassandraStore[_, _], (_) => Row)],
    // mapping from indexed table and the indexed column to the table containing the index and the 
    val indexes: Map[(AbstractCQLCassandraStore[_, _], String), (AbstractCQLCassandraStore[_, _], String)]
) extends QueryspaceConfiguration(name) {

  override def queryCanBeOrdered(query: Query): Option[ColumnConfiguration] = {
    query.getOrdering.flatMap { ordering =>
      val registry = Registry.querySpaceColumns.get(query.getQueryspace)
      registry.flatMap { reg =>
        reg.get(ordering.column).flatMap { colConfig => 
          colConfig.index.flatMap(index => if(index.isManuallyIndexed.isDefined && index.isSorted) {
            Some(colConfig)
          } else {
            None
          })
        }
      }
    }
  }
  
  override def queryCanBeGrouped(query: Query): Option[ColumnConfiguration] = queryCanBeOrdered(query)
  
  override def getColumns: List[ColumnConfiguration] = tables.toList.flatMap ( table => {
    val extractor = CassandraExtractor.getExtractor(table._1)
    val allColumns = extractor.getTableConfiguration(table._1).allColumns
    allColumns.map(col => extractor.getColumnConfiguration(table._1, col, this, index))
  })
  
  override def getTables: Set[scray.querying.description.TableConfiguration[_, _]] = tables.map ( table => {
    val extractor = CassandraExtractor.getExtractor(table._1)
    extractor.getTableConfiguration(table._1, table._2)
  })
}