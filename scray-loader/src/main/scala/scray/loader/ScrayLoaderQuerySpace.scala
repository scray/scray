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
package scray.loader

import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.internal.MaterializedView
import scray.querying.queries.DomainQuery
import scray.querying.description.ColumnConfiguration
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.loader.configparser.ScrayQueryspaceConfiguration
import scray.loader.configparser.ScrayConfiguration
import scray.loader.configuration.ScrayStores
import scala.collection.mutable.HashMap
import scray.querying.storeabstraction.StoreGenerators
import scray.querying.sync.DbSession
import scray.querying.description.ColumnConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.storeabstraction.StoreExtractor
import scray.querying.description.VersioningConfiguration
import scray.querying.description.Row
import scray.querying.description.Column
import scray.querying.source.store.QueryableStoreSource
import com.twitter.util.FuturePool
import scray.querying.description.IndexConfiguration
import scray.querying.Registry


/**
 * a generic query space that can be used to load tables from
 * various different databases.
 */
class ScrayLoaderQuerySpace(name: String, config: ScrayConfiguration, qsConfig: ScrayQueryspaceConfiguration,
    storeConfig: ScrayStores, futurePool: FuturePool) 
    extends QueryspaceConfiguration(name) with LazyLogging {

  val generators = new HashMap[String, StoreGenerators]
  val extractors = new HashMap[TableIdentifier, StoreExtractor[_]]
  storeConfig.addSessionChangeListener { (name, session) => generators -= name }
  
  val version = qsConfig.version
  
  /**
   * if this queryspace can order accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying the
   * results will be ordered if the queryspace can choose the main table
   */
  def queryCanBeOrdered(query: DomainQuery): Option[ColumnConfiguration] = {
    val orderingColumn = query.ordering match {
      case Some(columnOrdering) => Some(columnOrdering.column)
      case _ => None
    }
    orderingColumn.map {Registry.getQuerySpaceColumn(query.getQueryspace, query.querySpaceVersion, _) }.flatten
  }
  
  /**
   * if this queryspace can group accoring to query all by itself, i.e. 
   * without an extra in-memory step introduced by scray-querying
   */
  def queryCanBeGrouped(query: DomainQuery): Option[ColumnConfiguration] = {
    val groupingColumn = query.grouping match {
      case Some(grouping) => Some(grouping.column)
      case _ => None
    }
    groupingColumn.map {Registry.getQuerySpaceColumn(query.getQueryspace, query.querySpaceVersion, _) }.flatten
  }
  
  /**
   * If this queryspace can handle the query using the materialized view provided.
   * The weight (Int) is an indicator for the specificity of the view and reflects the 
   * number of columns that match query arguments.
   */
  def queryCanUseMaterializedView(query: DomainQuery, materializedView: MaterializedView): Option[(Boolean, Int)] = ???
  
  /**
   * return a generator for the given named dbms
   */
  private def getGenerator(dbmsId: String, session: DbSession[_, _, _]) = {
    generators.get(dbmsId).getOrElse {
      val generator = storeConfig.getStoreGenerator(dbmsId, session, name, futurePool)
      generators += ((dbmsId, generator))
      generator
    }
  }

  /**
   * return configuration for a simple rowstore
   */
  private def getRowstoreConfiguration(id: TableIdentifier): Option[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
    // Extractor
    def extractTable[Q <: DomainQuery, S <: QueryableStoreSource[Q]](storeconfigs: (S, 
        (Function1[_, Row], Option[String], Option[VersioningConfiguration[_, _]])), generator: StoreGenerators):
        TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _] = {
      // TODO: read latest version from SyncTable, if it is declared there, generate a VersioningConfig; otherwise leave it by None
      val extractor = generator.getExtractor[Q, S](storeconfigs._1, Some(id.tableId), None, Some(id.dbSystem), futurePool)
      val tid = extractor.getTableIdentifier(storeconfigs._1, storeconfigs._2._2, Some(id.dbSystem))
      extractors.+=((tid, extractor))
      extractor.getTableConfiguration(storeconfigs._2._1)
    } 
    // retrieve session...
    val session = storeConfig.getSessionForStore(id.dbSystem)
    // TODO: add session change listener to change store in case of session change
    // storeConfig.addSessionChangeListener(listener)
    session.flatMap { sess =>
      val generator = getGenerator(id.dbSystem, sess)
      val sStore = generator.createRowStore(id)
      sStore.map { storeconfigs =>
        extractTable(storeconfigs, generator)
      }
    }    
  }
  
  /**
   * returns configuration of tables which are included in this query space
   * Internal use! 
   */
  def getTables(version: Int): Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
    val generators = new HashMap[String, StoreGenerators]
    // TODO: read versioned tables from SyncTable and add to rowstores
    val rowstores = qsConfig.rowStores
    rowstores.map { tableConfigTxt =>
      getRowstoreConfiguration(tableConfigTxt)
    }.collect { 
      case Some(tableConfiguration) => tableConfiguration
    }.toSet
    // TODO: add more tables (for the ones in the queryspace config, e.g. indexes) 
  }
  
  /**
   * returns columns which can be included in this query space
   * Internal use! 
   */
  override def getColumns(version: Int): List[ColumnConfiguration] = {
    def getColumnConfig[S <: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]](table: S): List[ColumnConfiguration] = {
      def throwError: Exception = {
        logger.error("Store must be registered before columns can be extracted!")
        new UnsupportedOperationException("Store must be registered before columns can be extracted!") 
      }
      def extractTableConfig[Q <: DomainQuery, F <: QueryableStoreSource[Q]](column: Column, extractor: StoreExtractor[F]): ColumnConfiguration = {
        // TODO: add indexing configuration (replace maps)
        val index = extractor.createManualIndexConfiguration(column, name, version, table.queryableStore.get.asInstanceOf[F], Map(), Map())
        storeConfig.getSessionForStore(column.table.dbSystem).map { session =>
          // TODO: add splitter configuration
          extractor.getColumnConfiguration(session, column.table.dbId, column.table.tableId, column, index, Map())
        }.getOrElse(throw new DBMSUndefinedException(column.table.dbSystem, name))
      }
      table.allColumns.map { column =>
        // fetch extractor
        extractors.get(column.table).getOrElse(throw throwError) match {
          case extractor: StoreExtractor[s] => extractTableConfig[DomainQuery, QueryableStoreSource[DomainQuery]](column, extractor.asInstanceOf[StoreExtractor[QueryableStoreSource[DomainQuery]]])
          case _ => throw throwError
        }
      }.toList
    }
    val tables = getTables(version)
    tables.flatMap { table => getColumnConfig(table) }.toList 
  }
  
  /**
   * re-initialize this queryspace, possibly re-reading the configuration from somewhere
   */
  def reInitialize(oldversion: Int): QueryspaceConfiguration = ???
  
  
  override def toString: String = {
    s"""$name { tables: [${getTables(0)}] }"""
  }
}
