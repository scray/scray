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

/**
 * Registry for tables and resources 
 */
object Registry {

  // all querySpaces, that can be queried, TODO: make this thread safe, 
  // but not with ConcurrentHashmap, as this will read often !
  val querySpaces = new HashMap[String, QueryspaceConfiguration]
  
  // all tables must be registered here, query plugins do this
  // val tableRegistry = new HashMap[TableIdentifier, TableConfiguration[_]]
  
  // all columns
  // val columnRegistry = new HashMap[Column, ColumnConfiguration]
 
  // shortcut to find table-configurations
  val querySpaceTables = new HashMap[String, HashMap[TableIdentifier, TableConfiguration[_, _]]]
  
  // shortcut to find column-configurations
  val querySpaceColumns = new HashMap[String, HashMap[Column, ColumnConfiguration]]
  
  /**
   * Register a new querySpace
   * TODO: OSGIfy such that db- and query-plugins can use this
   */
  def registerQuerySpace(querySpace: QueryspaceConfiguration): Unit = {
    querySpaces.put(querySpace.name, querySpace)
    querySpaceColumns.put(querySpace.name, new HashMap[Column, ColumnConfiguration])
    querySpaceTables.put(querySpace.name, new HashMap[TableIdentifier, TableConfiguration[_, _]])
    querySpace.getColumns.foreach(col => querySpaceColumns.get(querySpace.name).map(_.put(col.column, col)))
      // columnRegistry.put(col.column, col)
    querySpace.getTables.foreach(table => querySpaceTables.get(querySpace.name).map(_.put(table.table, table)))
  }
}
