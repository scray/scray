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
package scray.querying.description

import com.twitter.storehaus.{QueryableStore, ReadableStore}
import scray.querying.Query
import scray.querying.queries.DomainQuery
import scray.querying.source.indexing.IndexConfig
import scray.querying.description.internal.MaterializedView
import scray.querying.source.pooling.StorePool

/**
 * parent class of queryspace-configuration, used to identify which tables
 * are part of which database system for a single sort of compiled query
 */
abstract class QueryspaceConfiguration(val name: String) {

  /**
   * if this queryspace can order accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying the
   * results will be ordered if the queryspace can choose the main table
   */
  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration]
  
  /**
   * if this queryspace can group accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying
   */
  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration]
  
  /**
   * If this queryspace can handle the query using the materialized view provided.
   * The weight (Int) is an indicator for the specificity of the view and reflects the 
   * number of columns that match query arguments.
   */
  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)]
  
  /**
   * returns configuration of tables which are included in this query space
   * Internal use! 
   */
  def getTables(version: Int): Set[TableConfiguration[_, _, _]]
  
  /**
   * returns columns which can be included in this query space
   * Internal use! 
   */
  def getColumns(version: Int): List[ColumnConfiguration]
  
  /**
   * re-initialize this queryspace, possibly re-reading the configuration from somewhere
   */
  def reInitialize(oldversion: Int): QueryspaceConfiguration
}


/**
 * configuration of a column, espc. whether and how it is indexed 
 */
case class ColumnConfiguration (
  column: Column,
  querySpace: QueryspaceConfiguration,
  index: Option[IndexConfiguration] // whether this column is indexed and how
)

/**
 * represents the configuration of an index for a single column
 */
case class IndexConfiguration (
  isAutoIndexed: Boolean, // indexed by means of the database, e.g. a B+ in Oracle
  isManuallyIndexed: Option[ManuallyIndexConfiguration[_, _, _, _, _]], // if this is a hand-made index, e.g. by means of Hadoop
  isSorted: Boolean, // if this is a sorted index, e.g. by means of a clustering key
  isGrouped: Boolean, // if this is a sorted index, we think of grouping as to be a sort without ordering requirements
  isRangeQueryable: Boolean, // if this index can be range queried
  autoIndexConfiguration: Option[AutoIndexConfiguration[_]] 
)

/**
 * represents auto-indexed columns with additional properties 
 */
case class AutoIndexConfiguration[T] (
  isRangeIndex: Boolean = false, // if ranges can be queried efficiently on this index 
  isFullTextIndex: Boolean = false, // whether we can perform queries like wildcard, phrase, etc. 
  isSorted: Boolean = false,
  rangePartioned: Option[((T, T), Boolean) => Iterator[(T, T)]] = None // if the range needs to be partitioned and how
)

/**
 * information we need about this manual index
 */
case class ManuallyIndexConfiguration[K, R, M, V, Q] (
  mainTableConfig: () => TableConfiguration[Q, R, V], // the table that holds all data
  indexTableConfig: () => TableConfiguration[Q, K, M], // the table that holds the index into the data of single column of mainTableConfig
  keymapper: Option[M => R], // map the type of the references from one table to the type of the keys of the other
  combinedIndexColumns: Set[Column], // if this is a combined index, this contains additional columns that must be present to use the index
  indexConfig: IndexConfig // depending on the type of the configuration we decide how to handle this
)

/**
 * properties of tables
 */
case class TableConfiguration[Q, K, V] (
  table: TableIdentifier,
  versioned: Option[VersioningConfiguration[Q, K, V]], // if the table is versioned and how
  primarykeyColumns: List[Column], // the primary key columns of the table, i.e. a unique reference into a row with partitioning relevance  
  clusteringKeyColumns: List[Column], // some more primary key columns which can be ordered
  allColumns: List[Column], // a List of all columns in the table
  rowMapper: (V) => Row, // mapper from a result row returned by the store to a scray-row
  domainQueryMapping: DomainQuery => Q, // maps a scray-DomainQuery to a query of the store
  queryableStore: Option[() => QueryableStore[Q, V]], // the queryable store representation, allowing to query the store, None if versioned
  readableStore: Option[() => ReadableStore[K, V]], // the readablestore, used in case this is used by a HashJoinSource, None if versioned
  materializedViews: List[MaterializedView] // materialized views for this table
)

/**
 * we generally assume versions to be Longs, see Summingbird for more information
 */
case class VersioningConfiguration[Q, K, V] (
//  latestCompleteVersion: () => Option[Long], // latest complete version of the table 
  runtimeVersion: () => Option[Long], // current real-time version, which is updated continuously
  nameVersionMapping: Option[(String, Long) => String], // if needed, a mapping for the table name for the version
  queryableStore: StorePool[QueryableStore[Q, V]], // the versioned queryable store representation, allowing to query the store
  readableStore: StorePool[ReadableStore[K, V]] // the versioned readablestore, used in case this is used by a HashJoinSource
//  queryableStore: Long => QueryableStore[Q, V], // the versioned queryable store representation, allowing to query the store
//  readableStore: Long => ReadableStore[K, V] // the versioned readablestore, used in case this is used by a HashJoinSource
//  dataVersionMapping: () // if needed, a mapping for the data to fit the version, not supported yet
)
