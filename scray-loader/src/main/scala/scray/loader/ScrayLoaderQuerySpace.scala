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
import scray.common.key.OrderedStringKeyGenerator
import scray.common.key.OrderedStringKeyGenerator
import scray.common.key.StringKey
import scray.common.errorhandling.ErrorHandler
import scray.common.errorhandling.ScrayProcessableErrors
import scray.common.errorhandling.LoadCycle
import scala.collection.mutable.ArrayBuffer
import scray.common.key.api.KeyGenerator


/**
 * a generic query space that can be used to load tables from
 * various different databases.
 */
class ScrayLoaderQuerySpace(name: String, config: ScrayConfiguration, val qsConfig: ScrayQueryspaceConfiguration,
    storeConfig: ScrayStores, futurePool: FuturePool, errorHandler: ErrorHandler) 
    extends QueryspaceConfiguration(name) with LazyLogging {

  val generators = new HashMap[String, StoreGenerators]
  val extractors = new HashMap[TableIdentifier, StoreExtractor[_]]
  storeConfig.addSessionChangeListener { (name, session) => generators -= name }
  
  val version = qsConfig.version
  
  val materializedViews = qsConfig.materializedViews.map { view => 
    if(view.keyClass.equals("scray.common.key.OrderedStringKeyGenerator")) {
      new MaterializedView(view.table, view.bundleOrJarFile, OrderedStringKeyGenerator) 
    } else {
      logger.warn("Unknown key generator class. use default: scray.common.key.OrderedStringKeyGenerator")
      new MaterializedView(view.table, view.bundleOrJarFile, OrderedStringKeyGenerator)
    }
  }
  
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
   * return a generator for the given named dbms
   */
  protected def getGenerator(dbmsId: String, session: DbSession[_, _, _]) = {
    generators.get(dbmsId).getOrElse {
      val generator = storeConfig.getStoreGenerator(dbmsId, session, name, futurePool)
      generators += ((dbmsId, generator))
      generator
    }
  }

  /**
   * return configuration for a simple rowstore
   */
  protected def getRowstoreConfiguration(id: TableIdentifier): Option[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
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
  override def getTables(version: Int): Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
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
  
  private def getColumnConfiguration4Column(column: Column, version: Int, table: Option[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]): ColumnConfiguration = {
    def extractTableConfig[Q <: DomainQuery, F <: QueryableStoreSource[Q]](column: Column, extractor: StoreExtractor[F]): ColumnConfiguration = {
      // TODO: add indexing configuration (replace maps)
      val index = table.flatMap { table => 
        extractor.createManualIndexConfiguration(column, name, version, table.queryableStore.get.asInstanceOf[F], Map(), Map())
      }
      storeConfig.getSessionForStore(column.table.dbSystem).map { session =>
        // TODO: add splitter configuration
        extractor.getColumnConfiguration(session, column.table.dbId, column.table.tableId, column, index, Map())
      }.getOrElse(throw new DBMSUndefinedException(column.table.dbSystem, name))
    }    
    def throwError: Exception = {
      logger.error("Store must be registered before columns can be extracted!")
      errorHandler.handleError(ScrayProcessableErrors.QS_DBMS_MISSING, LoadCycle.FIRST_LAOD, "Store must be registered before columns can be extracted!")
      new UnsupportedOperationException("Store must be registered before columns can be extracted!") 
    }
    // fetch extractor
    extractors.get(column.table).getOrElse(throw throwError) match {
      case extractor: StoreExtractor[s] => extractTableConfig[DomainQuery, QueryableStoreSource[DomainQuery]](column, extractor.asInstanceOf[StoreExtractor[QueryableStoreSource[DomainQuery]]])
      case _ => throw throwError
    }
  }
  
  /**
   * returns columns which can be included in this query space
   * Internal use! 
   */
  override def getColumns(version: Int): List[ColumnConfiguration] = {
    def getColumnConfig[S <: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]](table: S): List[ColumnConfiguration] = {      
      table.allColumns.map { column =>
        getColumnConfiguration4Column(column, version, Some(table))
      }.toList
    }
    val tables = getTables(version)
    tables.flatMap { table => getColumnConfig(table) }.toList 
  }

  /**
   * handles updates for materialized views
   */
  private def handleMaterializedViewsUpdates(newQSConfig: ScrayQueryspaceConfiguration, oldversion: Int, errorHandler: ErrorHandler): 
      (Boolean, Seq[MaterializedView]) = {
    var hasNewMaterials: Boolean = false
    val views = newQSConfig.materializedViews.flatMap { mv =>
      Registry.getMaterializedView(name, oldversion, mv.table).map { oldmv =>
        if(mv.keyClass != oldmv.keyGenerationClass || mv.bundleOrJarFile != oldmv.bundleNameOrJarFile) {
          if(mv.keyClass.trim == "") {
            Some(new MaterializedView(mv.table, mv.bundleOrJarFile, OrderedStringKeyGenerator))
          } else {
            val keyclass = ClassBundleLoaderAbstraction.getClassBundleLoaderAbstraction().
                loadClassOrBundle(mv.bundleOrJarFile, mv.keyClass, classOf[KeyGenerator[Array[String]]])
            errorHandler.handleError(ScrayProcessableErrors.QS_MV_FOUND, LoadCycle.RELOAD, s"key-class updated for materialized view: ${mv.table}")
            hasNewMaterials = true
            keyclass.map { kc => new MaterializedView(mv.table, mv.bundleOrJarFile, kc)}
          }
        } else {
          Some(oldmv)
        }
      }.getOrElse {
        if(mv.keyClass.trim == "") {
          Some(new MaterializedView(mv.table, mv.bundleOrJarFile, OrderedStringKeyGenerator))
        } else {
          val keyclass = ClassBundleLoaderAbstraction.getClassBundleLoaderAbstraction().
              loadClassOrBundle(mv.bundleOrJarFile, mv.keyClass, classOf[KeyGenerator[Array[String]]])
          errorHandler.handleError(ScrayProcessableErrors.QS_MV_FOUND, LoadCycle.RELOAD, s"New Materialized view ${mv.table}")
          hasNewMaterials = true
          keyclass.map { kc => new MaterializedView(mv.table, mv.bundleOrJarFile, kc) }
        }
      }
    }
    (hasNewMaterials, views)
  }

  
  /**
   * re-initialize this queryspace, possibly re-reading the configuration from somewhere
   */
  override def reInitialize(oldversion: Int, newConfig: Any, errorHandler: ErrorHandler): Option[QueryspaceConfiguration] = {
    
    // --- compare the configs ---
    val newQSConfig = newConfig.asInstanceOf[ScrayQueryspaceConfiguration]
    // name must be equal -> otherwise 
    if(newQSConfig.name != name) {
      errorHandler.handleError(ScrayProcessableErrors.QS_NAME_CHANGED, LoadCycle.RELOAD)
    }
    if(newQSConfig.version == oldversion) {
      // no version change -> no need for a new version
      return Some(this)
    }
    if(newQSConfig.version < oldversion) {
      // this is ridiculous -> new version should not be smaller than the old one
      errorHandler.handleError(ScrayProcessableErrors.QS_VERSION_DECREASED, LoadCycle.RELOAD)
    }
    
    // TODO: handle Indexstores
    // newQSConfig.indexStores
    
    // handle rowstores
    val (neednewspace, qstables, qscolumns) = handleRowStoreUpdates(newQSConfig, oldversion, errorHandler)
    
    // handle materialized views
    val (hasnewmaterials, materializedViews) = handleMaterializedViewsUpdates(newQSConfig, oldversion, errorHandler)
      
    if(neednewspace || hasnewmaterials) {
      Some(new UpdatedScrayLoaderQuerySpace(name, config, newQSConfig,
        storeConfig, futurePool, qstables, qscolumns, errorHandler))
    } else {
      // if no new queryspace is need we return None to denote "stay with the current one"
      None
    }
  }
  
  
  
  /**
   * classify rowstores into new ones and old ones which we might be able to reuse if everything
   * else in that table (columns, keys, indexes, etc.) remained the same
   */
  private def classifyRowStores(newtables: Seq[TableIdentifier], unchanged: Seq[TableIdentifier], newTIs: Seq[TableIdentifier]): 
      (Seq[TableIdentifier], Seq[TableIdentifier]) = {
    if(!newtables.isEmpty) {
      val newti = newtables.head
      qsConfig.rowStores.find(_ == newti).map { oldTi =>
        classifyRowStores(newtables.tail, unchanged :+ oldTi, newTIs)
      }.getOrElse {
        classifyRowStores(newtables.tail, unchanged, newTIs :+ newti)
      }
    } else {
      (unchanged, newTIs)
    }
  }

  /**
   * checks row stores and produces updates if needed
   */
  private def handleRowStoreUpdates(newQSConfig: ScrayQueryspaceConfiguration, oldversion: Int, errorHandler: ErrorHandler): 
    (Boolean, Set[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]], List[ColumnConfiguration]) = {
    
    val remainingColumns = new ArrayBuffer[ColumnConfiguration]()
    val (unchanged, newTIs) = classifyRowStores(newQSConfig.rowStores, Seq(), Seq())
    
    var changed: Boolean = false
    // 1. transfer all rowstores which remained the same
    val oldtables = unchanged.map { ti =>
      // check that the table has not changed, i.e. the columns remain the same
      val origTableOpt = Registry.getQuerySpaceTable(name, oldversion, ti)
      val newTableOpt = getRowstoreConfiguration(ti)
      if(newTableOpt.isDefined) {
        // check all columns
        val (oldColumns, newColumns, deletedColumns) = origTableOpt.map { origTable =>
          diffColumns(origTable.allColumns, newTableOpt.get.allColumns, Set[Column](), Set[Column]())
        }.getOrElse {
          (Set[Column](), newTableOpt.get.allColumns, Set[Column]())
        }
        oldColumns.foreach(col => remainingColumns += Registry.getQuerySpaceColumn(name, oldversion, col).get)
        newColumns.foreach(col => remainingColumns += getColumnConfiguration4Column(col, oldversion + 1, None))
        deletedColumns.foreach(col => errorHandler.handleError(ScrayProcessableErrors.QS_COLUMN_REMOVED, LoadCycle.RELOAD,
            s"Detected removal of column ${col} after reload"))
        // check if the key has changed
        val (oldRowColumns, newRowColumns, deletedRowColumns) = origTableOpt.map { origTable =>
          diffColumns(origTable.primarykeyColumns, newTableOpt.get.primarykeyColumns, Set[Column](), Set[Column]())
        }.getOrElse {
          (Set[Column](), newTableOpt.get.primarykeyColumns, Set[Column]())
        }
        oldRowColumns.foreach(col => remainingColumns += Registry.getQuerySpaceColumn(name, oldversion, col).get)
        newRowColumns.foreach { col =>
          remainingColumns += getColumnConfiguration4Column(col, oldversion + 1, None)
          errorHandler.handleError(ScrayProcessableErrors.QS_COLUMN_FOUND, LoadCycle.RELOAD, s"Detected new row-key ${col} after reload")
        }
        deletedRowColumns.foreach(col => errorHandler.handleError(ScrayProcessableErrors.QS_COLUMN_REMOVED, LoadCycle.RELOAD,
            s"Detected removal of row-key-column ${col} after reload"))
        // check if clustering key has changed
        val (oldClusterColumns, newClusterColumns, deletedClusterColumns) = origTableOpt.map { origTable =>
          diffColumns(origTable.clusteringKeyColumns, newTableOpt.get.clusteringKeyColumns, Set[Column](), Set[Column]())
        }.getOrElse {
          (Set[Column](), newTableOpt.get.clusteringKeyColumns, Set[Column]())
        }
        oldClusterColumns.foreach(col => remainingColumns += Registry.getQuerySpaceColumn(name, oldversion, col).get)
        newClusterColumns.foreach { col => 
          remainingColumns += getColumnConfiguration4Column(col, oldversion + 1, None)
          errorHandler.handleError(ScrayProcessableErrors.QS_COLUMN_FOUND, LoadCycle.RELOAD, s"Detected new cluster-key ${col} after reload")
        }
        deletedClusterColumns.foreach(col => errorHandler.handleError(ScrayProcessableErrors.QS_COLUMN_REMOVED, LoadCycle.RELOAD,
            s"Detected removal of cluster-key-column ${col} after reload"))
        // decide if we need a new TableConfiguration
        if(!newColumns.isEmpty || !deletedColumns.isEmpty || 
            !newRowColumns.isEmpty || !deletedRowColumns.isEmpty ||
            !newClusterColumns.isEmpty || !deletedClusterColumns.isEmpty) {
          changed = true
          val newTable = newTableOpt.get
          origTableOpt.map { origTable =>
            // some columns have changed -> create new one using the ols columns wherever possible
            tableCopy(newTable, origTable, oldColumns, newColumns, oldClusterColumns, newClusterColumns, oldRowColumns, newRowColumns)
          }.getOrElse(newTableOpt.get)
        } else {
          // everything remained the same -> use old one
          logger.debug(s"Table ${ti} remained the same. Reusing previous table definition")
          origTableOpt.foreach(origTable => origTable.allColumns.foreach { col => 
            remainingColumns += Registry.getQuerySpaceColumn(name, oldversion, col).get
          })
          origTableOpt.getOrElse {
            changed = true
            newTableOpt.get
          }
        }
      } else {
        errorHandler.handleError(ScrayProcessableErrors.QS_TABLE_MISSING, LoadCycle.RELOAD, 
            s"Table ${ti} is not available right now, re-using previous table configuration, if available")
        origTableOpt.getOrElse {
          errorHandler.handleError(ScrayProcessableErrors.QS_TABLE_MISSING, LoadCycle.RELOAD, 
            s"No previous table definition for ${ti} available, re-using failed")
          null
        }
      }
    }.toSet
    val newtables = newTIs.flatMap { ti => getRowstoreConfiguration(ti) }.toSet
    ((!newTIs.isEmpty || changed), oldtables ++ newtables, remainingColumns.toList)
  }

  
  /**
   * fills a TableConfiguration by copying a previous existing class
   */
  private def tableCopy[QS1 <: DomainQuery, QK1 <: DomainQuery, V1, QS2 <: DomainQuery, QK2 <: DomainQuery, V2](
      newTable: TableConfiguration[QS1, QK1, V1], origTable: TableConfiguration[QS2, QK2, V2],
      oldColumns: Set[Column], newColumns: Set[Column], oldClusterColumns: Set[Column], newClusterColumns: Set[Column],
      oldRowColumns: Set[Column], newRowColumns: Set[Column]): TableConfiguration[QS2, QK2, V2] = {
    val newTableConverted = newTable.asInstanceOf[TableConfiguration[QS2, QK2, V2]]
    val rowKeys = origTable.copy(primarykeyColumns = (oldRowColumns ++ newRowColumns))
    val clustKeys = rowKeys.copy(clusteringKeyColumns = (oldClusterColumns ++ newClusterColumns))
    val allCols = clustKeys.copy(allColumns = (oldColumns ++ newColumns))
    val queryStore = allCols.copy(queryableStore = newTableConverted.queryableStore)
    queryStore.copy(readableStore = newTableConverted.readableStore)
  }
    
  /**
   * creates a diff between two sets of Columns (1 and 2) and returns a Tuple3 with
   * a set of old columns (which are in both sets), a set of new columns (which are only in set 2)
   * and a set of deleted columns (which can be found only in set 1)
   */
  private def diffColumns(origTable: Set[Column], newTable: Set[Column], 
        newColumns: Set[Column], oldColumns: Set[Column]): (Set[Column], Set[Column], Set[Column]) = 
    if(newTable.isEmpty) {
      // compute deleted columns 
      val deletedColumns = origTable.collect {
        case oldCol if !oldColumns.contains(oldCol) => oldCol
      }
      (oldColumns, newColumns, deletedColumns)
    } else {
      val column = newTable.head
      if(origTable.contains(column)) {
        diffColumns(origTable, newTable.tail, newColumns, oldColumns + origTable.find(_ == column).get)
      } else {
        diffColumns(origTable, newTable.tail, newColumns + column, oldColumns)
      }
    }
  
  
  def getMaterializedViews: Seq[MaterializedView] = materializedViews
  
  override def toString: String = {
    s"""$name { tables: [${getTables(0)}] }"""
  }
}

