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
 * MariaDB dialect for Scray
 */
object MariaDBDialect extends ScraySQLDialect("MARIADB") {

    /**
   * MariaDB implements limits by LIMIT [<offset> ,] <limit>
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]]) = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    if(range.skip.isDefined || range.limit.isDefined) {
      sbuf.append(" LIMIT ")
      range.skip.foreach { skip =>
        // offsets / skips start from zero in mysql
        sbuf.append(s"${skip}")
      }
      if(range.skip.isDefined && range.limit.isDefined) {
        sbuf.append(", ")
      }
      if(range.skip.isDefined && !range.limit.isDefined) {
        // according to mysql docu append large number to retrieve 
        // all rows if skip will be defined only
        sbuf.append("18446744073709551615")
      } else {
        sbuf.append(s"${range.limit.get}")
      }
    }
    (sbuf.toString, List())
  }.getOrElse(("", List()))

  /**
   * we scan if the URL is of format:
   * jdbc:mysql://...
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:MARIADB://")
  
  override val DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver"
}