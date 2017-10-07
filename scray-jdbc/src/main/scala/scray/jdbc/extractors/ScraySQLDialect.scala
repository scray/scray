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
package scray.jdbc.extractors

import scray.querying.description.internal.Domain
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange

/**
 * Ancestor of all SQL dialects that implement relational DBMS dialects
 */
abstract class ScraySQLDialect(val name: String, unequal: String = "<>") {
  
  /**
   * returns true if the provided jdbcURL is valid for this type of dialect
   */
  def isDialectJdbcURL(jdbcURL: String): Boolean
  
  /**
   * returns the name of this dialect for extraction
   */
  def getName: String = name
  
  /**
   * returns the unequal operator for this relational DBMS
   */
  def getUnequal: String = unequal
  
  /**
   * removes quotes for this relational DBMS, using a default, which removes " and ; and '
   */
  def removeQuotes(in: String): String = in.filterNot(c => c == '"' || c == ';' || c == ''')
  
  /**
   * Returns a formatted Select String for this relational DBMS
   * Warning: default assumption is, that where, group by, order by and limit clauses are directly consecutive
   * as specified in this list
   */
  def getFormattedSelectString(table: TableIdentifier, where: String, limit: String, 
      groupBy: String, orderBy: String): String =
    s"""SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy} ${limit}"""

  /**
   * limits are usually non-standard for DBMS systems, so we leave the implementation
   * open to enforce implementation in concrete dialects
   */
  def getEnforcedLimit(range: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]])
  
  /**
   * if the empty String should be consered to be equal to NULL values
   */
  def emptyStringIsNull: Boolean = false
  
  /**
   * decide whether to place a WHERE in front of the domains
   */
  def decideWhere(where: String): String = if(!where.isEmpty()) s"WHERE $where" else ""

  /**
   * class name of the driver for the database of this dialect
   */
  val DRIVER_CLASS_NAME: String
}