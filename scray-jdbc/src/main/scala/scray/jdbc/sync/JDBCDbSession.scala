package scray.jdbc.sync


import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.slf4j.LazyLogging
import com.zaxxer.hikari.HikariDataSource

import scray.querying.sync.DbSession
import scray.querying.sync.StatementExecutionError
import com.zaxxer.hikari.HikariConfig
import scray.jdbc.extractors.ScraySQLDialect
import scray.jdbc.extractors.ScraySQLDialectFactory

/**
 * Session implementation for JDBC datasources using a Hikari connection pool
 */
class JDBCDbSession(val ds: HikariDataSource, val metadataConnection: Connection, val sqlDialiect: ScraySQLDialect) extends DbSession[PreparedStatement, PreparedStatement, ResultSet](ds.getJdbcUrl) with LazyLogging{
  
  def this(ds: HikariDataSource, sqlDialiect: ScraySQLDialect) = this(ds, ds.getConnection, sqlDialiect)
  
  def this(jdbcURL: String, sqlDialect: ScraySQLDialect, username: String, password: String) = {
    this({
        val config = new HikariConfig()
        config.setDriverClassName(sqlDialect.DRIVER_CLASS_NAME)
        config.setJdbcUrl(jdbcURL)
        config.setUsername(username)
        config.setPassword(password)
        new HikariDataSource(config)
      }, sqlDialect
    )
  }
  
  def this(jdbcURL: String, username: String, password: String) =
    this(jdbcURL, ScraySQLDialectFactory.getDialectFromJdbcURL(jdbcURL), username, password)
  
  override def execute(statement: String): Try[ResultSet] = {
      try {
        val prepStatement = metadataConnection.prepareStatement(statement)
        val result = prepStatement.executeQuery()
        Success(result)
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

  override def execute(statement: PreparedStatement): Try[ResultSet] = {
      try {
        val result = statement.executeQuery()
        Success(result)
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

  
  override def insert(statement: PreparedStatement): Try[ResultSet] = {
      try {
        logger.debug("Insert " + statement)
        val result = statement.executeUpdate()
        if(result > 0) {
         Success(null)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

   def printStatement(s: Statement): String = {
     def produceWarnings(warning: SQLWarning): String = {
       Option(warning.getNextWarning).map(warning => warning.getLocalizedMessage + ";" + produceWarnings(warning.getNextWarning)).getOrElse("")
     }
     s match {
         case bStatement: PreparedStatement  => "It is currently not possible to execute : " + produceWarnings(bStatement.getWarnings)
         case _                              => "It is currently not possible to execute : " + s
     }
   }
}