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

import scala.collection.mutable.HashMap
import scray.querying.description.{
  Column,
  ColumnConfiguration,
  QueryspaceConfiguration,
  TableConfiguration,
  TableIdentifier
}
import scray.querying.planning.PostPlanningActions
import java.util.concurrent.locks.{
  ReadWriteLock,
  ReentrantReadWriteLock,
  ReentrantLock
}
import scray.querying.source.Source
import org.mapdb.HTreeMap
import scray.querying.caching.Cache
import scray.querying.caching.serialization.RegisterRowCachingSerializers
import scray.querying.monitoring.Monitor
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.caching.MonitoringInfos
import java.util.concurrent.atomic.AtomicBoolean
import scray.querying.queries.QueryInformation
import java.util.UUID
import com.twitter.util.JavaTimer
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import com.twitter.util.Time
import scala.collection.mutable.ArrayBuffer
import scray.querying.monitoring.MonitorQuery

/**
 * default trait to represent get operations on the registry
 */
trait Registry {

  /**
   * returns the current queryspace configuration
   */
  @inline def getQuerySpace(space: String): Option[QueryspaceConfiguration]

  /**
   * returns a column configuration
   */
  @inline def getQuerySpaceColumn(space: String, column: Column): Option[ColumnConfiguration]

  /**
   * returns a table configuration
   */
  @inline def getQuerySpaceTable(space: String, ti: TableIdentifier): Option[TableConfiguration[_, _, _]]
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

  private val enableCaches = new AtomicBoolean(true)

  // information about queries
  private val queryMonitor = new HashMap[UUID, QueryInformation]
  private val queryMonitorRwLock = new ReentrantReadWriteLock

  /**
   * returns the current queryspace configuration
   * Cannot be used to query the Registry for tables or columns of a queryspace,
   * because of concurrent updates. Use more specific methods instead.
   */
  @inline override def getQuerySpace(space: String): Option[QueryspaceConfiguration] = {
    rwlock.readLock.lock
    try {
      querySpaces.get(space)
    } finally {
      rwlock.readLock.unlock
    }
  }

  // shortcut to find table-configurations
  private val querySpaceTables = new HashMap[String, HashMap[TableIdentifier, TableConfiguration[_, _, _]]]

  /**
   * returns a table configuration
   */
  @inline def getQuerySpaceTable(space: String, ti: TableIdentifier): Option[TableConfiguration[_, _, _]] = {
    rwlock.readLock.lock
    try {
      querySpaceTables.get(space).flatMap(_.get(ti))
    } finally {
      rwlock.readLock.unlock
    }
  }

  // shortcut to find column-configurations
  private val querySpaceColumns = new HashMap[String, HashMap[Column, ColumnConfiguration]]

  /**
   * returns a column configuration
   */
  @inline override def getQuerySpaceColumn(space: String, column: Column): Option[ColumnConfiguration] = {
    rwlock.readLock.lock
    try {
      querySpaceColumns.get(space).flatMap(_.get(column))
    } finally {
      rwlock.readLock.unlock
    }
  }

  /**
   * Register a new querySpace
   */
  def registerQuerySpace(querySpace: QueryspaceConfiguration): Unit = {
    rwlock.writeLock.lock
    try {
      querySpaces.put(querySpace.name, querySpace)
      querySpaceColumns.put(querySpace.name, new HashMap[Column, ColumnConfiguration])
      querySpaceTables.put(querySpace.name, new HashMap[TableIdentifier, TableConfiguration[_, _, _]])
      querySpace.getColumns.foreach(col => querySpaceColumns.get(querySpace.name).map(_.put(col.column, col)))
      querySpace.getTables.foreach(table => querySpaceTables.get(querySpace.name).map(_.put(table.table, table)))
    } finally {
      rwlock.writeLock.unlock
    }
    monitor.monitor(querySpaceTables)
  }



  /**
   * return a "private" copy of a query space in this registry to be used
   * without synchronization. The planner will attach the returned objects to
   * each DomainQuery for easy access. Concurrent modifications of the registry
   * will therefore only marginally affect running queries (changes to mutable
   * list will.
   */
  def getRegistryQueryspaceCopy(querySpace: String): Registry = {
    rwlock.readLock.lock
    try {
      new Registry {
        private val columns = querySpaceColumns.get(querySpace).map(_.clone()).getOrElse(new HashMap())
        private val tables = querySpaceTables.get(querySpace).map(_.clone()).getOrElse(new HashMap())
        @inline def getQuerySpace(space: String): Option[QueryspaceConfiguration] = {
          None
        }
        @inline def getQuerySpaceColumn(space: String, column: Column): Option[ColumnConfiguration] = {
          space match {
            case `querySpace` => columns.get(column)
            case _ => None
          }
        }
        @inline def getQuerySpaceTable(space: String, ti: TableIdentifier): Option[TableConfiguration[_, _, _]] = {
          space match {
            case `querySpace` => tables.get(ti)
            case _ => None
          }
        }
      }
    } finally {
      rwlock.readLock.unlock
    }
  }

  /**
   * Must be called to update the table and columns information. It suffices to update columns which
   * actually have been updated. Does not update the queryspace-object itself - only the information
   * that is really used by the planner.
   */
  def updateTableInformation(
      querySpace: String,
      tableid: TableIdentifier,
      tableconfig: TableConfiguration[_ , _, _],
      columConfigsToUpdate: List[ColumnConfiguration] = List()) = {
    rwlock.writeLock.lock
    try {
      querySpaceTables.get(querySpace).map(_.put(tableid, tableconfig))
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
  def setCachingEnabled(enabled: Boolean) = enableCaches.set(enabled)
  def getCachingEnabled = enableCaches.get

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

  val cleanupQueryInformation = new JavaTimer(true).schedule(Duration.fromTimeUnit(1, TimeUnit.HOURS)) {
    queryMonitorRwLock.writeLock().lock()
    try {
      val cutoffTime = Time.now - Duration.fromTimeUnit(1, TimeUnit.HOURS)
      val qMon = queryMonitor.filterNot { entry =>
        entry._2.finished.get > 0 && entry._2.finished.get < cutoffTime.inMillis  ||     // finished more than one hour ago
        entry._2.pollingTime.get > 0 && entry._2.pollingTime.get < cutoffTime.inMillis } // probably query has died
      (queryMonitor -- qMon.keys).map(_._2.destroy())
      queryMonitor.clear()
      queryMonitor ++= qMon
    } finally {
      queryMonitorRwLock.writeLock().unlock()
    }
  }
}
