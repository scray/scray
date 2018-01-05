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
import scala.collection.mutable.ListBuffer
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.Column

/**
 * Oracle dialect for Scray
 */
object ScrayOracleDialect extends ScraySQLDialect("ORACLE") {
  
  // only used to set limits, which are handled differently
  private val dummyTi = TableIdentifier("", "", "")
  
  /**
   * Oracle implements limits by introducing an implicit column
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]]) = rangeOpt.map { range =>
    val domainBuf = new ListBuffer[Domain[_]]
    val sbuf = new StringBuffer
    // if we have entries in the list, then we do not need the "and" upfront...
    range.skip.foreach { skip =>
      sbuf.append(">")
      // name of column doesn't matter, so use dummy
      domainBuf.+=(new SingleValueDomain[Int](Column("", dummyTi), skip.toInt))
    }
    range.limit.foreach { limit =>
      sbuf.append("<")
      // name of column doesn't matter, so use dummy
      domainBuf.+=(new SingleValueDomain[Int](Column("", dummyTi), limit.toInt))
    }
    (sbuf.toString, domainBuf.toList)    
  }.getOrElse(("", List()))

  /**
   * Because Oracle has a special way of handling limits we need a special SELECT clause for it
   */
  override def getFormattedSelectString(table: TableIdentifier, where: String, limit: String,
      groupBy: String, orderBy: String): String =
    if (limit.length == 0) {
      s"""SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy} """
    } else {
      if(limit.trim() == ("><")) { 
        s"""SELECT * FROM (
            SELECT a.*, rownum rnum FROM (
                SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy}) a 
                WHERE rownum <= ?)
            WHERE rnum >= ?"""
      } else {
        if(limit.trim() == ">") {
          s"""SELECT * FROM (
            SELECT a.*, rownum rnum FROM (
                SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy}) a) 
            WHERE rnum >= ?"""
        } else {
          s"""SELECT * FROM (SELECT a.*, rownum rnum FROM (SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${groupBy} ${orderBy}) a WHERE rownum <= ?)"""          
        }
      }
    }

  
  /**
   * Oracle has the special overly bad habit of treating empty Strings as to be NULLs
   * TODO: Probably we need to account for that in the query generation! 
   */
  override def emptyStringIsNull: Boolean = true
  
  /**
   * we scan if the URL is of format:
   * jdbc:oracle:thin:...
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:ORACLE:THIN:")
  
  override val DRIVER_CLASS_NAME = "oracle.jdbc.OracleDriver"
}
