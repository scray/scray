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

import scray.querying.description.Clause
import java.util.UUID
import scray.querying.description.ColumnGrouping
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange
import scray.querying.description.Columns
import scray.querying.description.ColumnOrdering


/**
 * represents all possible queries
 */
trait Query extends Serializable {

  /**
   * query id, used to debug the query and query maintenance (e.g. status or kill)
   */
  def getQueryID: UUID
  
  /**
   * the name of the query space this query should be executed reside in
   */
  def getQueryspace: String
  
  /**
   * columns in the result set
   */
  def getResultSetColumns: Columns
  
  /**
   * table description in which to look for data
   */
  def getTableIdentifier: TableIdentifier
  
  /**
   * returns an AST in prefix-notation of the where condition of this query
   */
  def getWhereAST: Option[Clause]
  
  /**
   * whether or not the results should be grouped after a column
   */
  def getGrouping: Option[ColumnGrouping]
  
  /**
   * whether or not the results should be ordered according to a single column
   */
  def getOrdering: Option[ColumnOrdering[_]]
  
  /**
   * whether or not this query returns an interval over the result set
   */
  def getQueryRange: Option[QueryRange]
  
  /**
   * returns a modified query
   */
  def transformedAstCopy(ast: Option[Clause]): Query
}
