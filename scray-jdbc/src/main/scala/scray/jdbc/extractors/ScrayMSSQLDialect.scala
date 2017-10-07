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
 * MSSQL dialect for Scray
 */
object ScrayMSSQLDialect extends ScraySQLDialect("MSSQL") {
  
  /**
   * MSSQL implements limits by offset and fetch
   * Warning: can only be applied if there will be an order by clause
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]]) = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    if(range.skip.isDefined || range.limit.isDefined) {
      sbuf.append(s" OFFSET ${range.skip.getOrElse(0L)} ROWS ")
      range.limit.foreach { limit =>
        sbuf.append(s" FETCH NEXT ${limit} ROWS ONLY ")
      }
    }
    (sbuf.toString, List())
  }.getOrElse(("", List()))

  /**
   * we scan if the URL is of format:
   * jdbc:sqlserver://...
   * 
   * correct format according to Microsoft is:
   * jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;user=MyUserName;password=*****;
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:SQLSERVER://")
  
  override val DRIVER_CLASS_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}
