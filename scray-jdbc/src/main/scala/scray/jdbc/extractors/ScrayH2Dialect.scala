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
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): String = rangeOpt.map { range =>
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
    sbuf.toString
  }.getOrElse("")

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
