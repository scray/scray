package scray.jdbc.extractors

import scray.querying.description.internal.Domain
import scray.querying.description.TableIdentifier
import scray.querying.description.QueryRange

/**
 * Oracle dialect for Scray
 */
object ScrayOracleDialect extends ScraySQLDialect("ORACLE") {
  
  /**
   * Oracle implements limits by introducing an implicit column
   */
  override def getEnforcedLimit(rangeOpt: Option[QueryRange], where: List[Domain[_]]): String = rangeOpt.map { range =>
    val sbuf = new StringBuffer
    // if we have entries in the list, then we do not need the "and" upfront...
    if(where.size > 0) {
      sbuf.append(DomainToSQLQueryMapping.AND_LITERAL)
    } else {
      sbuf.append(" WHERE ")
    }
    range.skip.foreach { skip =>
      sbuf.append(s" ROWNUM > $skip ")
    }
    if(range.skip.isDefined && range.limit.isDefined) {
      sbuf.append(DomainToSQLQueryMapping.AND_LITERAL)
    }
    range.limit.foreach { limit =>
      sbuf.append(s" ROWNUM <= $limit ")
    }
    sbuf.toString
    
  }.getOrElse("")

  /**
   * Because Oracle has a special way of handling limits we need a special SELECT clause for it
   */
  override def getFormattedSelectString(table: TableIdentifier, where: String, limit: String,
      groupBy: String, orderBy: String): String =
    s"""SELECT * FROM "${removeQuotes(table.dbId)}"."${removeQuotes(table.tableId)}" ${decideWhere(where)} ${limit} ${groupBy} ${orderBy} """

  
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
