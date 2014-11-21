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
package scray.cassandra.extractors

import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scalaz.Memo
import scray.querying.description.ColumnConfiguration
import scray.querying.description.IndexConfiguration
import scray.querying.description.QueryspaceConfiguration
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import com.datastax.driver.core.KeyspaceMetadata
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.IndexConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.Row
import com.websudos.phantom.CassandraPrimitive
import scray.querying.description.EmptyRow
import scray.querying.description.EmptyRow
import scray.querying.queries.DomainQuery
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore

/**
 * Extractor object for Storehaus'-CQLCassandraCollectionStores
 */
class CQLCollectionStoreExtractor[S <: CQLCassandraCollectionStore[_, _, _, _, _, _]](store: S, tableName: Option[String]) 
    extends CassandraExtractor[S] {

  override def getColumns: List[Column] = 
    getInternalColumns(store, tableName, store.rowkeyColumnNames ++ store.colkeyColumnNames ++ List(store.valueColumnName))
  
  override def getClusteringKeyColumns: List[Column] =
    getInternalColumns(store, tableName, store.colkeyColumnNames)

  override def getRowKeyColumn: Column =
    getInternalColumns(store, tableName, List(store.rowkeyColumnNames.head)).head
  
  override def getRowKeyColumns: List[Column] =
    getInternalColumns(store, tableName, store.rowkeyColumnNames)
  
  override def getValueColumns: List[Column] =
    getInternalColumns(store, tableName, List(store.valueColumnName))
    
  override def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_, _, _] = {
    TableConfiguration[Any, Any, Any] (
      getTableIdentifier(store, tableName), 
      // TODO: add versioning information here
      None,
      getRowKeyColumn,
      getClusteringKeyColumns,
      getColumns,
      rowMapper.asInstanceOf[(Any) => Row],
      getQueryMapping(store),
      () => store.asInstanceOf[QueryableStore[Any, Any]],
      () => store.asInstanceOf[ReadableStore[Any, Any]]
    )
  }
}
