package scray.jdbc.extractors

import scray.querying.description.internal.Domain
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange

/**
 * MySQL dialect for Scray
 */
object ScrayMySQLDialect extends ScraySQLDialect("MYSQL") {
  
  /**
   * MySQL implements limits by LIMIT [<offset> ,] <limit>
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
    jdbcURL.toUpperCase().startsWith("JDBC:MYSQL://")
  
  override val DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver"
}
