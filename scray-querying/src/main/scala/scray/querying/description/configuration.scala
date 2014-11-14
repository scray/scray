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

import scray.querying.Query
import scray.querying.queries.DomainQuery
import com.twitter.storehaus.QueryableStore
import com.twitter.storehaus.ReadableStore

/**
 * parent class of queryspace-configuration, used to identify which tables
 * are part of which database system for a single sort of compiled query
 */
abstract class QueryspaceConfiguration(val name: String) {

  /**
   * if this queryspace can order accoring to query by itself
   */
  def queryCanBeOrdered(query: Query): Option[ColumnConfiguration]
  
  /**
   * if this queryspace can group accoring to query by itself
   */
  def queryCanBeGrouped(query: Query): Option[ColumnConfiguration]
  
  /**
   * returns configuration of tables which are included in this query sapce
   */
  def getTables: Set[TableConfiguration[_, _]]
  
  /**
   * returns columns which can be included in this query space
   */
  def getColumns: List[ColumnConfiguration]
  
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
  isManuallyIndexed: Option[ManuallyIndexConfiguration], // if this is a hand-made index, e.g. by means of Hadoop
  isSorted: Boolean, // if this is a sorted index, e.g. by means of a clustering key
  isGrouped: Boolean, // if this is a sorted index, we think of grouping as to be a sort without ordering requirements
  isRangeQueryable: Boolean // if this index can be range queried
) 

/**
 * information we need about this manual index
 */
case class ManuallyIndexConfiguration (
  mainTableConfig: TableConfiguration[_, _], // the table that holds all data
  indexTableConfig: TableConfiguration[_, _] // the table that holds the index into the data of single column of mainTableConfig
)

/**
 * properties of tables
 */
case class TableConfiguration[V, Q] (
  table: TableIdentifier,
  versioned: Option[VersioningConfiguration], // if the table is versioned and how
  primarykeyColumn: Column, // the primary key columns of the table, i.e. a unique reference into a row with partitioning relevance  
  clusteringKeyColumns: List[Column], // some more primary key columns which can be ordered
  allColumns: List[Column], // a List of all columns in the table
  rowMapper: (V) => Row, // mapper from a result row returned by the store to a scray-row
  domainQueryMapping: DomainQuery => Q, // maps a scray-DomainQuery to a query of the store
  queryableStore: () => QueryableStore[Q, V], // the queryable store representation, allowing to query the store
  readableStore: () => ReadableStore[Q, V] // the readablestore, used in case this is used by a HashJoinSource
)

/**
 * we generally assume versions to be Longs, see Summingbird for more information
 */
case class VersioningConfiguration (
  latestCompleteVersion: () => Long, // latest complete version of the table 
  runtimeVersion: () => Long, // current real-time version, which is updated continuously
  nameVersionMapping: Option[(String, Long) => String] // if needed, a mapping for the table name for the version
  // dataVersionMapping: () // if needed, a mapping for the data to fit the version, not supported yet
)
