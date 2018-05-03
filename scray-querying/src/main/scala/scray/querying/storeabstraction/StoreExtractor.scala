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
package scray.querying.storeabstraction

import scray.querying.description.VersioningConfiguration
import java.util.regex.Pattern
import scray.querying.queries.DomainQuery
import scray.querying.source.indexing.IndexConfig
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.IndexConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.ColumnConfiguration
import scray.querying.description.AutoIndexConfiguration
import scray.querying.description.Row
import scray.querying.description.Column
import scray.querying.source.Splitter
import scray.querying.Registry
import scray.querying.sync.DbSession
import scray.querying.source.store.QueryableStoreSource


/**
 * extracts meta-information from a given store
 */
trait StoreExtractor[S <: QueryableStoreSource[_]] {
 
  /**
   * returns a list of columns for this specific store; implementors must override this
   */
  def getColumns: Set[Column]
  
  /**
   * returns list of clustering key columns for this specific store; implementors must override this
   */
  def getClusteringKeyColumns: Set[Column]
  
  /**
   * returns list of row key columns for this specific store; implementors must override this
   */
  def getRowKeyColumns: Set[Column]

  /**
   * returns a list of value key columns for this specific store; implementors must override this
   */
  def getValueColumns: Set[Column]

  /**
   * returns the table configuration for this specific store; implementors must override this
   */
  def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]

  /**
   * DB-System is fixed
   */
  def getDefaultDBSystem: String
  
  /**
   * returns a table identifier for this store
   */
  def getTableIdentifier(store: S, tableName: Option[String], dbSystem: Option[String]): TableIdentifier
    
  /**
   * returns the column configuration for a column
   */
//  def getColumnConfiguration(store: S, 
//      column: Column,
//      querySpace: QueryspaceConfiguration,
//      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]],
//      splitters: Map[Column, Splitter[_]]): ColumnConfiguration
  
  def getColumnConfiguration(session: DbSession[_, _, _, _],
      dbName: String,
      table: String,
      column: Column,
      index: Option[ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]],
      splitters: Map[Column, Splitter[_]]): ColumnConfiguration
      
  /**
   * returns all column configurations
   */
  def getColumnConfigurations(session: DbSession[_, _, _, _],
      dbName: String,
      table: String,
      querySpace: QueryspaceConfiguration, 
      indexes: Map[String, ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]],
      splitters: Map[Column, Splitter[_]]): Set[ColumnConfiguration] = {
    getColumns.map(col => getColumnConfiguration(session, dbName, table, col, indexes.get(col.columnName), splitters))
  }
  
  /**
   * return a manual index configuration for a column
   */
  def createManualIndexConfiguration(column: Column, queryspaceName: String, version: Int, store: S,
      indexes: Map[_ <: (QueryableStoreSource[_ <: DomainQuery], String), _ <: (QueryableStoreSource[_ <: DomainQuery], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[_ <: QueryableStoreSource[_], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _]])]):
        Option[ManuallyIndexConfiguration[_ <: DomainQuery, _ <: DomainQuery, _, _, _ <: DomainQuery]]
  
  private def getTableConfigurationFunction[Q <: DomainQuery, K <: DomainQuery, V](ti: TableIdentifier, space: String, version: Int): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, version, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
}

