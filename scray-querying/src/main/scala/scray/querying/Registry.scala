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
import scray.querying.description.Column
import scray.querying.description.ColumnConfiguration
import scray.querying.description.QueryspaceConfiguration
import scray.querying.description.TableConfiguration
import scray.querying.description.TableIdentifier
import scray.querying.planning.PostPlanningActions
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scray.querying.description.ColumnConfiguration
import scray.querying.source.Source
import org.mapdb.HTreeMap
import scray.querying.caching.Cache
import java.util.concurrent.locks.ReentrantLock
import scray.querying.caching.serialization.RegisterRowCachingSerializers

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
object Registry extends Registry {

  // makes registry thread safe at the cost of some performance;
  // however, reads should not be blocking each other
  private val rwlock = new ReentrantReadWriteLock
  
  // all querySpaces, that can be queried
  private val querySpaces = new HashMap[String, QueryspaceConfiguration]
  
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
   * replace the cache with a new one
   */
  def replaceCache[T](cacheDiscriminant: String, oldCache: Option[Cache[T]], newCache: Cache[T]): Unit = {
    cachelock.lock
    try {
      oldCache.map(_.close)
      caches.put(cacheDiscriminant, newCache)
    } finally {
      cachelock.unlock
    }    
  }
}
