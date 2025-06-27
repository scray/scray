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
package scray.querying

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import com.twitter.util.Time
import com.twitter.util.Try
import com.typesafe.scalalogging.LazyLogging

import scray.querying.caching.Cache
import scray.querying.caching.MonitoringInfos
import scray.querying.description.Column
import scray.querying.description.ColumnConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.MaterializedView
import scray.querying.monitoring.Monitor
import scray.querying.monitoring.MonitorQuery
import scray.querying.planning.PostPlanningActions
import scray.querying.queries.DomainQuery
import scray.querying.queries.QueryInformation
import scray.querying.source.Source

/**
 * default trait to represent get operations on the registry
 */
trait Registry {

  /**
   * returns the current queryspace configuration
   */
  @inline def getQuerySpace(space: String, version: Int): Option[QueryspaceConfiguration]

  /**
   * returns a column configuration
   */
  @inline def getQuerySpaceColumn(space: String, version: Int, column: Column): Option[ColumnConfiguration]

  /**
   * returns a table configuration
   */
  @inline def getQuerySpaceTable(space: String, version: Int, ti: TableIdentifier): Option[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]
  
  /**
   * returns the latest version of a given query space
   */
  @inline def getLatestVersion(space: String): Option[Int]

  /**
   * returns all available version of a given query space
   */
  @inline def getVersions(space: String): Set[Int]
  
  /**
   * return a metrializedView
   */
  @inline def getMaterializedView(space: String, version: Integer, ti: TableIdentifier): Option[MaterializedView]
}


/**
 * Registry for tables and resources
 */
object Registry extends LazyLogging with Registry {

  // Object to send monitor information to
  private val monitor = new Monitor

  private val createQueryInformationListeners = new ArrayBuffer[QueryInformation => Unit]

  private val querymonitor = new MonitorQuery

  
  // makes registry thread safe at the cost of some performance;
  // however, reads should not be blocking each other
  private val rwlock = new ReentrantReadWriteLock

  // all querySpaces, that can be queried
  private val querySpaces = new HashMap[String, QueryspaceConfiguration]
  private val querySpaceVersions = new HashMap[String, Set[Int]]

  private val enableCaches = new AtomicBoolean(true)

  // information about queries
  private val queryMonitor = new HashMap[UUID, QueryInformation]
  private val queryMonitorRwLock = new ReentrantReadWriteLock

  @inline def getQuerySpaceNames(): List[String] = querySpaceVersions.map(_._1).toList
  
  /**
   * returns the latest version of a given query space
   */
  @inline override def getLatestVersion(space: String): Option[Int] = Try(getVersions(space).max).toOption

  /**
   * returns all available version of a given query space
   */
  @inline override def getVersions(space: String): Set[Int] = {
    rwlock.readLock.lock
    try {
      querySpaceVersions.get(space).getOrElse(Set())      
    } finally {
      rwlock.readLock().unlock()
    }
  }
  
  /**
   * returns the current queryspace configuration
   * Cannot be used to query the Registry for tables or columns of a queryspace,
   * because of concurrent updates. Use more specific methods instead.
   */
  @inline override def getQuerySpace(space: String, version: Int): Option[QueryspaceConfiguration] = {
    rwlock.readLock.lock
    try {
      querySpaces.get(space + version)
    } finally {
      rwlock.readLock.unlock
    }
  }

  // shortcut to find table-configurations
  private val querySpaceTables = new HashMap[String, HashMap[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]]

  @inline def getQuerySpaceTables(space: String, version: Int): Map[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
    rwlock.readLock.lock
    try {
      logger.trace("Query table" + space + version + " Existing tables: " + querySpaceTables.keySet)
      querySpaceTables.get(space + version).map(_.toMap).getOrElse(Map())
    } finally {
      rwlock.readLock.unlock
    }    
  }
  
  /**
   * returns a table configuration
   */
  @inline def getQuerySpaceTable(space: String, version: Int, ti: TableIdentifier): Option[TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]] = {
    rwlock.readLock.lock
    try {
      val qt = querySpaceTables.get(space + version)
      logger.trace(s"Search table with identifier ${ti} in dataset ${qt}")
      qt.flatMap(_.get(ti))
    } finally {
      rwlock.readLock.unlock
    }
  }

  // shortcut to find column-configurations
  private val querySpaceColumns = new HashMap[String, HashMap[Column, ColumnConfiguration]]

  /**
   * returns a column configuration
   */
  @inline override def getQuerySpaceColumn(space: String, version: Int, column: Column): Option[ColumnConfiguration] = {
    rwlock.readLock.lock
    try {
      querySpaceColumns.get(space + version).flatMap(_.get(column))
    } finally {
      rwlock.readLock.unlock
    }
  }

  /**
   * Register a querySpace, producing a new version
   */
  def registerQuerySpace(querySpace: QueryspaceConfiguration, version: Option[Int] = None): Int = {
    rwlock.writeLock.lock
    try {
      val newVersion = version.orElse(getLatestVersion(querySpace.name).map(_ + 1)).getOrElse(0)
      querySpaces.put(querySpace.name + newVersion, querySpace)
      querySpaceColumns.put(querySpace.name + newVersion, new HashMap[Column, ColumnConfiguration])
      querySpaceTables.put(querySpace.name + newVersion, new HashMap[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]])
      querySpace.getColumns(newVersion).foreach(col => querySpaceColumns.get(querySpace.name + newVersion).map(_.put(col.column, col)))
      querySpace.getTables(newVersion).foreach(table => querySpaceTables.get(querySpace.name + newVersion).map(_.put(table.table, table)))
      querySpaceVersions.put(querySpace.name, querySpaceVersions.get(querySpace.name).getOrElse(Set()) + newVersion)
      logger.debug(s"Registered query space ${querySpaces.get(querySpace.name + newVersion)}")
      
      // Register materialized views
      this.materializedViews.put(querySpace.name + newVersion, new HashMap[TableIdentifier, MaterializedView])
      querySpace.getMaterializedViews().map { mv => this.materializedViews.get(querySpace.name + newVersion).get.put(mv.table, mv)}
      
      newVersion
    } finally {
      rwlock.writeLock.unlock
      monitor.monitor(querySpaceTables)
    }
  }

  /**
   * Must be called to update tables and columns information. It suffices to update columns which
   * actually have been updated. Does not update the queryspace-object itself - only the information
   * that is really used by the planner is given a new version.
   * TODO: find a mechanism to throw out old versions
   */
  def updateQuerySpace(querySpace: String, tables: Set[(TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _], List[ColumnConfiguration])]): Unit = {
    if(tables.size > 0) {
      rwlock.writeLock.lock
      try {
        // get is o.k. since this method may not be called if the qs has not been previously created
        val oldVersion = getLatestVersion(querySpace).getOrElse(0)
        val newVersion = oldVersion + 1
        logger.debug(s"Creating new version for query-space $querySpace, updating $oldVersion to $newVersion by providing ${tables.size} new tables.")
        // copy the stuff over...
        querySpaceTables.get(querySpace + oldVersion).map { qtables =>
          val newQuerySpaceTables = new HashMap[TableIdentifier, TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _]]
          newQuerySpaceTables ++= qtables
          querySpaceTables.put(querySpace + newVersion, newQuerySpaceTables)
        }
        querySpaceColumns.get(querySpace + oldVersion).map { qcolumns =>
          val newQuerySpaceColumns = new HashMap[Column, ColumnConfiguration]
          newQuerySpaceColumns ++= qcolumns
          querySpaceColumns.put(querySpace + newVersion, newQuerySpaceColumns)
        }
        // replace old with new ones
        tables.foreach(table => updateTableInformation(querySpace, newVersion, table._1, table._2, table._3))
      } finally {
        rwlock.writeLock.unlock
      }
    }
  }
  
  /**
   * Must be called to update the table and columns information. It suffices to update columns which
   * actually have been updated. Does not update the queryspace-object itself - only the information
   * that is really used by the planner.
   */
  private def updateTableInformation(
      querySpace: String,
      version: Int,
      tableid: TableIdentifier,
      tableconfig: TableConfiguration[_ <: DomainQuery, _ <: DomainQuery, _],
      columConfigsToUpdate: List[ColumnConfiguration] = List()): Unit = {
    rwlock.writeLock.lock
    try {
      querySpaceTables.get(querySpace + version).map(_.put(tableid, tableconfig))
      columConfigsToUpdate.foreach(col => querySpaceColumns.get(querySpace).map(_.put(col.column, col)))
      // TODO: invalidate relevant caches, if these exist in the future :)
    } finally {
      rwlock.writeLock.unlock
    }
  }


  // planner post-pocessor
  var queryPostProcessor: PostPlanningActions.PostPlanningAction = PostPlanningActions.doNothing

  private val cachelock = new ReentrantLock
  private val caches = new HashMap[String, Cache[_]]

  /**
   * retrieve an off-heap cache for reading
   */
  def getCache[T, C <: Cache[T]](source: Source[_, _]): C = {
    cachelock.lock
    try {
      caches.get(source.getDiscriminant).getOrElse {
        val newCache = source.createCache
        caches.put(source.getDiscriminant, newCache)
        newCache
      }.asInstanceOf[C]
    } finally {
      cachelock.unlock
    }
  }

  /**
   * return cache for given discriminant if it exists
   */
  def getCache[T, C <: Cache[T]](cacheID: String): Option[C] = {
    cachelock.lock
    try {
      caches.get(cacheID).asInstanceOf[Option[C]]
    } finally {
      cachelock.unlock
    }
  }

  /**
   * replace the cache with a new one
   */
  def replaceCache[T](cacheID: String, oldCache: Option[Cache[T]], newCache: Cache[T]): Unit = {
    cachelock.lock
    try {
      oldCache.map(_.close)
      caches.put(cacheID, newCache)
    } finally {
      cachelock.unlock
    }
  }

  /**
   * Get cache information
   */
  def getCacheCounter[T, C <: Cache[T]](cacheID: String): Option[MonitoringInfos] = {
    cachelock.lock
    try {
      caches.get(cacheID).map(_.report)
    } finally {
      cachelock.unlock
    }
  }

  /**
   * en- or disable caching of column family values. Disable in case of memory pressure.
   */
  def setCachingEnabled(enabled: Boolean): Unit = enableCaches.set(enabled)
  def getCachingEnabled: Boolean = enableCaches.get

  def addCreateQueryInformationListener(listener: QueryInformation => Unit) =
    createQueryInformationListeners += listener

  def createQueryInformation(query: Query): QueryInformation = {
    queryMonitorRwLock.writeLock().lock()
    try {
      val info = new QueryInformation(query.getQueryID, query.getTableIdentifier, query.getWhereAST)
      queryMonitor += ((query.getQueryID, info))
      createQueryInformationListeners.foreach(_(info))
      info
    } finally {
      queryMonitorRwLock.writeLock().unlock()
    }
  }

  def getQueryInformation(qid: UUID): Option[QueryInformation] = {
    queryMonitorRwLock.readLock().lock()
    try {
      queryMonitor.get(qid)
    } finally {
      queryMonitorRwLock.readLock().unlock()
    }
  }

  val cleanupQueryInformation = new JavaTimer(true).schedule(Duration.fromTimeUnit(15, TimeUnit.MINUTES)) {
    queryMonitorRwLock.writeLock().lock()
    try {
      val cutoffTime = Time.now - Duration.fromTimeUnit(1, TimeUnit.HOURS)
      val qMon = queryMonitor.filter { entry =>
        ((entry._2.finished.get > 0) && (entry._2.finished.get < cutoffTime.inMillis))  ||      // finished more than one hour ago
        ((entry._2.pollingTime.get > 0) && (entry._2.pollingTime.get < cutoffTime.inMillis)) || // probably query has died
        ((entry._2.pollingTime.get == -1) && (entry._2.startTime < cutoffTime.inMillis)) }      // probably query has died without a single result
      qMon.foreach{ entry => 
        entry._2.destroy()
        queryMonitor -= entry._1
      }
    } finally {
      queryMonitorRwLock.writeLock().unlock()
    }
  }
  
  // shortcut to find materialized views
  private val materializedViews = new HashMap[String, HashMap[TableIdentifier, MaterializedView]]
  
  @inline def getMaterializedView(space: String, version: Integer, ti: TableIdentifier): Option[MaterializedView] = {
    rwlock.readLock.lock
    try {
      val mv = materializedViews.get(space + version)
      logger.debug(s"Search materialized view table with identifier ${ti} in dataset ${mv}")
      mv.flatMap(_.get(ti))
    } finally {
      rwlock.readLock.unlock
    }
  }

}
