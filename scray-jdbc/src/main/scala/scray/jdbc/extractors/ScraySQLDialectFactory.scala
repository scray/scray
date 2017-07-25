package scray.jdbc.extractors

/**
 * factory for creating ScraySQLDialect dialects by providing the config String
 * of the DBMS name
 */
object ScraySQLDialectFactory {
  
  def getDialectFromJdbcURL(url: String): ScraySQLDialect = {
    knownDialects.find(_.isDialectJdbcURL(url)).
      getOrElse(throw new AbstractMethodError(s"Scray SQL Dialect for jdbc URL '${url}' is unknown"))
  }
  
  def getDialect(name: String): ScraySQLDialect = 
    knownDialects.find(_.getName == name.toUpperCase()).
      getOrElse(throw new AbstractMethodError(s"Scray SQL Dialect '${name}' is unknown")) 
  
   // using only Hive so far not Spark   
  //val knownDialects = List(ScrayOracleDialect, ScrayHanaDialect, ScrayMySQLDialect, ScrayMSSQLDialect, ScrayHiveDialect, ScraySparkDialect, ScrayH2Dialect)
  val knownDialects = List(ScrayOracleDialect, ScrayHanaDialect, ScrayMySQLDialect, ScrayMSSQLDialect, ScrayHiveDialect, ScrayH2Dialect)
}