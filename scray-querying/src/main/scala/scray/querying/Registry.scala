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

/**
 * Registry for tables and resources
 */
object Registry {

  // makes registry thread safe at the cost of some performance;
  // however, reads should not be blocking each other
  private val rwlock = new ReentrantReadWriteLock
  
  // all querySpaces, that can be queried
  private val querySpaces = new HashMap[String, QueryspaceConfiguration]
  
  /**
   * returns the current queryspace configuration
   */
  @inline def getQuerySpace(space: String): Option[QueryspaceConfiguration] = {
    rwlock.readLock.lock
    try {
      querySpaces.get(space)
    } finally {
      rwlock.readLock.unlock
    }
  }
   
  // shortcut to find table-configurations
  private val querySpaceTables = new HashMap[String, HashMap[TableIdentifier, TableConfiguration[_, _]]]

  /**
   * returns a table configuration
   */
  @inline def getQuerySpaceTable(space: String, ti: TableIdentifier): Option[TableConfiguration[_, _]] = {
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
  @inline def getQuerySpaceColumn(space: String, column: Column): Option[ColumnConfiguration] = {
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
      querySpaceTables.put(querySpace.name, new HashMap[TableIdentifier, TableConfiguration[_, _]])
      querySpace.getColumns.foreach(col => querySpaceColumns.get(querySpace.name).map(_.put(col.column, col)))
        // columnRegistry.put(col.column, col)
      querySpace.getTables.foreach(table => querySpaceTables.get(querySpace.name).map(_.put(table.table, table)))
    } finally {
      rwlock.writeLock.unlock
    }
  }
  
  /**
   * Must be called to update the information. It suffices to update columns which actually have been 
   * updated. Does not update the queryspace-object itself - only the information that is really used
   * by the planner.
   */
  def updateTableInformation(
      querySpace: String,
      tableid: TableIdentifier,
      tableconfig: TableConfiguration[_ , _],
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
}
