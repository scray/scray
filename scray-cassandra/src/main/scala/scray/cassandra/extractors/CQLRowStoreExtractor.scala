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

import com.twitter.storehaus.cassandra.cql.CQLCassandraRowStore
import scray.querying.description.TableConfiguration
import scray.querying.description.Column
import scray.querying.description.Row
import com.twitter.storehaus.ReadableStore
import com.twitter.storehaus.QueryableStore

/**
 * Extractor object for Storehaus'-CQLCassandraCollectionStores
 */
class CQLRowStoreExtractor[S <: CQLCassandraRowStore[_]](store: S, tableName: Option[String]) extends CassandraExtractor[S] {

  override def getColumns: List[Column] =
    getInternalColumns(store, tableName, store.columns.map(_._1))
  
  override def getClusteringKeyColumns: List[Column] = List()

  override def getRowKeyColumn: Column =
    getInternalColumns(store, tableName, List(store.keyColumnName)).head
  
  override def getRowKeyColumns: List[Column] =
    getInternalColumns(store, tableName, List(store.keyColumnName))
  
  override def getValueColumns: List[Column] =
    getInternalColumns(store, tableName, store.columns.map(_._1).filterNot(_ == store.keyColumnName))
    
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
