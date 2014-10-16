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

import com.datastax.driver.core.KeyspaceMetadata
import com.twitter.storehaus.cassandra.cql.AbstractCQLCassandraStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraCollectionStore
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import scray.querying.description.Column
import scray.querying.description.ManuallyIndexConfiguration
import scray.querying.description.ColumnConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.IndexConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.TableConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.Row
import scray.querying.queries.DomainQuery
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.SingleValueDomain

/**
 * Helper class to create a configuration for a Cassandra table
 */
trait CassandraExtractor {

  /**
   * returns a list of columns for this specific store; implementors must override this
   */
  def getColumns(store: AbstractCQLCassandraStore[_, _]): List[Column]

  /**
   * DB-System is fixed
   */
  def getDBSystem: String = { "cassandra" }
  
  /**
   * returns a table identifier for this cassandra store
   */
  def getTableIdentifier(store: AbstractCQLCassandraStore[_, _]): TableIdentifier =
    TableIdentifier(getDBSystem, store.columnFamily.session.keyspacename, store.columnFamily.getName)

  /**
   * returns metadata information from Cassandra
   */
  def getMetadata(cf: StoreColumnFamily): KeyspaceMetadata = {
    cf.session.getSession.getCluster().getMetadata().getKeyspace(cf.session.getKeyspacename)
  }
  
  /**
   * checks that a column has been indexed by Cassandra itself, so no manual indexing
   */
  def checkColumnCassandraAutoIndexed(store: AbstractCQLCassandraStore[_, _], column: Column): Boolean = {
    Option(getMetadata(store.columnFamily).getTable(store.columnFamily.getName).getColumn(column.columnName).getIndex()).isDefined
  }

  /**
   * returns the column configuration for a Cassandra column
   */
  def getColumnConfiguration(store: AbstractCQLCassandraStore[_, _], 
      column: Column,
      querySpace: QueryspaceConfiguration,
      index: Option[ManuallyIndexConfiguration]): ColumnConfiguration = {
    val indexConfig = index match {
      case None => if(checkColumnCassandraAutoIndexed(store, column)) {
          Some(IndexConfiguration(true, None, false, false, false)) 
        } else { None }
      case Some(idx) => Some(IndexConfiguration(true, Some(idx), true, true, true)) 
    }
    ColumnConfiguration(column, querySpace, indexConfig)
  }
  
  /**
   * returns all column configurations
   */
  def getColumnConfigurations(store: AbstractCQLCassandraStore[_, _],
      querySpace: QueryspaceConfiguration, 
      indexes: Map[String, ManuallyIndexConfiguration]): List[ColumnConfiguration] = {
    getColumns(store).map(col => getColumnConfiguration(store, col, querySpace, indexes.get(col.columnName)))
  }
  
  /**
   * helper method to create a list of columns from a store
   */
  protected def getInternalColumns(store: AbstractCQLCassandraStore[_, _], colNames: List[String]) = {
    val ti = getTableIdentifier(store)
    colNames.map(Column(_, ti))    
  } 
  
  def getQueryMapping(store: AbstractCQLCassandraStore[_, _]): DomainQuery => String =
    DomainToCQLQueryMapper.getQueryMapping(store, this)
  
  def getClusteringKeyColumns(store: AbstractCQLCassandraStore[_, _]): List[Column]
  
  def getRowKeyColumn(store: AbstractCQLCassandraStore[_, _]): Column

  def getRowKeyColumns(store: AbstractCQLCassandraStore[_, _]): List[Column]
  
  def getValueColumns(store: AbstractCQLCassandraStore[_, _]): List[Column]
  
  def getTableConfiguration(store: AbstractCQLCassandraStore[_, _], rowMapper: (_) => Row): TableConfiguration[_, _]

}

object CassandraExtractor {
  
  /**
   * returns a Cassandra information extractor for a given Cassandra-Storehaus wrapper
   */
  def getExtractor(store: AbstractCQLCassandraStore[_, _]): CassandraExtractor = {
    store match { 
      case collStore: CQLCassandraCollectionStore[_, _, _, _, _, _] => CQLCollectionStoreExtractor
      
    }
  }
  
  
}
