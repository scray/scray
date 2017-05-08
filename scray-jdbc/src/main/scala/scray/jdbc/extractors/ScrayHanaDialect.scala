package scray.jdbc.extractors

import scray.querying.description.internal.Domain
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange

/**
 * Hana dialect for Scray
 */
object ScrayHanaDialect extends ScraySQLDialect("HANA") {
  
  /**
   * Hana implements limits by LIMIT <limit> [ OFFSET <offset> ]
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): String = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    if(range.skip.isDefined || range.limit.isDefined) {
      sbuf.append(" LIMIT ")
      if(range.skip.isDefined && !range.limit.isDefined) {
        // according to mysql docu append large number to retrieve 
        // all rows if skip will be defined only
        sbuf.append("18446744073709551615")
      } else {
        sbuf.append(s"${range.limit.get}")
      }
      range.skip.foreach { skip =>
        // offsets / skips start from zero in mysql
        sbuf.append(s" OFFSET ${skip}")
      }
    }
    sbuf.toString
  }.getOrElse("")

  /**
   * we scan if the URL is of format:
   * jdbc:sap://...
   * 
   * correct format according to SAP is:
   * jdbc:sap://localhost:30013/?databaseName=tdb1
   * and with failover:
   * jdbc:sap://myServer:30015;failover1:30015;failover2:30015/?autocommit=false
   */
  override def isDialectJdbcURL(jdbcURL: String): Boolean =
    jdbcURL.toUpperCase().startsWith("JDBC:SAP://")
  
  override val DRIVER_CLASS_NAME = "com.sap.db.jdbc.Driver"
}
