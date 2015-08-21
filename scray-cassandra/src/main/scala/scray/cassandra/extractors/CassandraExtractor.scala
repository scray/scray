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
import scray.querying.description.IndexConfiguration
import scray.querying.description.ManuallyIndexConfiguration
import com.twitter.storehaus.cassandra.cql.CQLCassandraStoreTupleValues
import com.datastax.driver.core.Metadata
import scray.querying.source.indexing.IndexConfig
import com.twitter.storehaus.cassandra.cql.CQLCassandraRowStore
import scray.cassandra.util.CassandraUtils
import scray.querying.description.VersioningConfiguration
import scray.querying.Registry

/**
 * Helper class to create a configuration for a Cassandra table
 */
trait CassandraExtractor[S <: AbstractCQLCassandraStore[_, _]] {

  /**
   * returns a list of columns for this specific store; implementors must override this
   */
  def getColumns: List[Column]
  
  /**
   * returns list of clustering key columns for this specific store; implementors must override this
   */
  def getClusteringKeyColumns: List[Column]
  
  /**
   * if this store is used as a ha-join reference it returns the (only) significant row-key
   */
  def getRowKeyColumn: Column

  /**
   * returns list of row key columns for this specific store; implementors must override this
   */
  def getRowKeyColumns: List[Column]

  /**
   * returns a list of value key columns for this specific store; implementors must override this
   */
  def getValueColumns: List[Column]

  /**
   * returns the table configuration for this specific store; implementors must override this
   */
  def getTableConfiguration(rowMapper: (_) => Row): TableConfiguration[_, _, _]

  /**
   * returns a generic Cassandra-store query mapping
   */
  def getQueryMapping(store: S, tableName: Option[String]): DomainQuery => String =
    new DomainToCQLQueryMapper[S].getQueryMapping(store, this, tableName)

  /**
   * DB-System is fixed
   */
  def getDBSystem: String = CassandraExtractor.DB_ID
  
  /**
   * returns a table identifier for this cassandra store
   */
  def getTableIdentifier(store: S, tableName: Option[String]): TableIdentifier =
    tableName.map(TableIdentifier(getDBSystem, store.columnFamily.session.keyspacename, _)).
      getOrElse(TableIdentifier(getDBSystem, store.columnFamily.session.keyspacename, store.columnFamily.getName))
    

  /**
   * returns metadata information from Cassandra
   */
  def getMetadata(cf: StoreColumnFamily): KeyspaceMetadata = CassandraUtils.getKeyspaceMetadata(cf)
  
  /**
   * checks that a column has been indexed by Cassandra itself, so no manual indexing
   * if the table has not yet been created (the version must still be computed) we assume no indexing 
   */
  def checkColumnCassandraAutoIndexed(store: S, column: Column): Boolean = {
    val metadata = Option(getMetadata(store.columnFamily))
    metadata.flatMap{_ => 
      val tm = CassandraUtils.getTableMetadata(store.columnFamily, metadata)
      val cm = Option(tm).map(_.getColumn(Metadata.quote(column.columnName)))
      cm.flatMap(colmeta => Option(colmeta.getIndex()))}.isDefined
  }

  /**
   * returns the column configuration for a Cassandra column
   */
  def getColumnConfiguration(store: S, 
      column: Column,
      querySpace: QueryspaceConfiguration,
      index: Option[ManuallyIndexConfiguration[_, _, _, _, _]]): ColumnConfiguration = {
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
  def getColumnConfigurations(store: S,
      querySpace: QueryspaceConfiguration, 
      indexes: Map[String, ManuallyIndexConfiguration[_, _, _, _, _]]): List[ColumnConfiguration] = {
    getColumns.map(col => getColumnConfiguration(store, col, querySpace, indexes.get(col.columnName)))
  }
  
  /**
   * helper method to create a list of columns from a store
   */
  protected def getInternalColumns(store: S, tableName: Option[String], colNames: List[String]) = {
    val ti = getTableIdentifier(store, tableName)
    colNames.map(Column(_, ti))    
  }
  
  /**
   * return a manual index configuration for a column
   */
  def createManualIndexConfiguration(column: Column, queryspaceName: String,
      store: S,
      indexes: Map[(AbstractCQLCassandraStore[_, _], String), (AbstractCQLCassandraStore[_, _], String, 
              IndexConfig, Option[Function1[_,_]], Set[String])],
      mappers: Map[AbstractCQLCassandraStore[_, _], ((_) => Row, Option[String], Option[VersioningConfiguration[_, _, _]])]):
        Option[ManuallyIndexConfiguration[_, _, _, _, _]] = {
    indexes.get((store, column.columnName)).map { (index) =>
      // TODO: fix this ugly stuff (for now we leave it, as fixing this will only increase type safety)
      val indexStore = index._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]]
      val indexstoreinfo = mappers.get(indexStore).get
      val indexExtractor = CassandraExtractor.getExtractor(indexStore, indexstoreinfo._2, indexstoreinfo._3)
      val storeinfo = mappers.get(store).get
      val mainDataTableTI = getTableIdentifier(store, None)
      ManuallyIndexConfiguration[Any, Any, Any, Any, Any](
        () => getTableConfigurationFunction[Any, Any, Any](mainDataTableTI, queryspaceName),
        () => getTableConfigurationFunction[Any, Any, Any](
            indexExtractor.getTableIdentifier(index._1.asInstanceOf[AbstractCQLCassandraStore[Any, Any]], indexstoreinfo._2), queryspaceName),
        index._4.asInstanceOf[Option[Any => Any]],
        index._5.map(Column(_, mainDataTableTI)),
        index._3
      )
    }
  }
  
  private def getTableConfigurationFunction[Q, K, V](ti: TableIdentifier, space: String): TableConfiguration[Q, K, V] = 
    Registry.getQuerySpaceTable(space, ti).get.asInstanceOf[TableConfiguration[Q, K, V]]
}

object CassandraExtractor {
  
  /**
   * returns a Cassandra information extractor for a given Cassandra-Storehaus wrapper
   */
  def getExtractor[S <: AbstractCQLCassandraStore[_, _]](store: S, tableName: Option[String],
          versions: Option[VersioningConfiguration[_, _, _]]): CassandraExtractor[S] = {
    store match { 
      case collStore: CQLCassandraCollectionStore[_, _, _, _, _, _] => 
        new CQLCollectionStoreExtractor(collStore, tableName, versions).asInstanceOf[CassandraExtractor[S]]
      case tupleStore: CQLCassandraStoreTupleValues[_, _, _, _] =>
        new CQLStoreTupleValuesExtractor(tupleStore, tableName, versions).asInstanceOf[CassandraExtractor[S]]
      case rowStore: CQLCassandraRowStore[_] =>
        new CQLRowStoreExtractor(rowStore, tableName, versions).asInstanceOf[CassandraExtractor[S]]
    }
  }
  
  val DB_ID: String = "cassandra"
}
