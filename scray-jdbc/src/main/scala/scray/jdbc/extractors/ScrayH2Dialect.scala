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
 * H2 dialect for Scray
 */
object ScrayH2Dialect extends ScraySQLDialect("H2") {
  
  /**
   * H2 implements limits by limit and offset (if limit has been specified) or offset and fetch
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): (String, List[Domain[_]]) = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    if(range.skip.isDefined || range.limit.isDefined) {
      range.limit.map { limit =>
        sbuf.append(s" LIMIT ${limit} ")
        range.skip.foreach { skip =>
          sbuf.append(s" OFFSET ${skip} ")
        }
      }.getOrElse {
        sbuf.append(s" OFFSET ${range.skip.getOrElse(0L)} ROWS ")
      }
    }
    (sbuf.toString, List())
  }.getOrElse(("", List()))

  /**
   * we scan if the URL is of format:
   * jdbc:h2:...
   * 
   * correct format according to H2 website is:
   * jdbc:h2:tcp://<server>[:<port>]/[<path>]<databaseName>
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:H2:")
  
  override val DRIVER_CLASS_NAME = "org.h2.Driver"
}
