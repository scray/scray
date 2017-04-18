package scray.jdbc.extractors

/**
 * factory for creating ScraySQLDialect dialects by providing the config String
 * of the DBMS name
 */
object ScraySQLDialectFactory {
  
  def getDialect(name: String): ScraySQLDialect = name.toUpperCase() match {
    case ORACLE => ScrayOracleDialect
    case _ => throw new AbstractMethodError(s"Scray SQL Dialect '${name}' is unknown") 
  }
  
  val ORACLE = "ORACLE"
  val MYSQL = "MYSQL"
  val HANA = "HANA"
  
}