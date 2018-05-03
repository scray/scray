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
package scray.jdbc.sync


import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLWarning
import java.sql.Statement

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import com.typesafe.scalalogging.LazyLogging
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import scray.jdbc.extractors.MariaDBDialect
import scray.jdbc.extractors.ScrayH2Dialect
import scray.jdbc.extractors.ScraySQLDialect
import scray.jdbc.extractors.ScraySQLDialectFactory
import slick.dbio.DBIOAction
import slick.jdbc.JdbcProfile
import slick.sql.FixedSqlAction
import scray.querying.sync.StatementExecutionError
import scray.querying.sync.DbSession

/**
 * Session implementation for JDBC datasources using a Hikari connection pool
 */
class JDBCDbSession(val ds: HikariDataSource, val metadataConnection: Connection, val sqlDialiect: ScraySQLDialect) extends DbSession[PreparedStatement, PreparedStatement, ResultSet, JdbcProfile](ds.getJdbcUrl) with LazyLogging {
  
  def this(ds: HikariDataSource, sqlDialiect: ScraySQLDialect) = this(ds, ds.getConnection, sqlDialiect)
  
  def this(jdbcURL: String, sqlDialect: ScraySQLDialect, username: String, password: String) = {
    this({
        val config = new HikariConfig()
        config.setDriverClassName(sqlDialect.DRIVER_CLASS_NAME)
        config.setJdbcUrl(jdbcURL)
        config.setUsername(username)
        config.setPassword(password)
        config.setMaximumPoolSize(3)
        new HikariDataSource(config)
      }, sqlDialect
    )
  }

 lazy val db = this.getConnectionInformations.get.api.Database.forDataSource(ds, Some(20))

  override def getConnectionInformations: Option[JdbcProfile] = {
    sqlDialiect  match {
      case MariaDBDialect => Some(slick.jdbc.MySQLProfile)
      case ScrayH2Dialect => Some(slick.jdbc.H2Profile)
      case _ => {
        logger.warn(s"No JdbcProfile for ${sqlDialiect} found.")
        None
      }
    }
  }
  
  def this(jdbcURL: String, username: String, password: String) =
    this(jdbcURL, ScraySQLDialectFactory.getDialectFromJdbcURL(jdbcURL), username, password)
  
 def executeQuery(statement: String): Try[ResultSet] = {
      try {
        val prepStatement = metadataConnection.prepareStatement(statement)
        val result = prepStatement.executeQuery()
        Success(result)
      } catch {
        case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    override def execute(statement: String) = {
      try {
        val prepStatement = metadataConnection.prepareStatement(statement)
        Try(prepStatement.execute)
      } catch {
        case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }
    
  override def execute(statement: PreparedStatement): Try[ResultSet] = {
      try {
        val result = statement.executeQuery()
        Success(result)
      } catch {
        case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }


    def execute[A, B <: slick.dbio.NoStream, C <: Nothing](statement: FixedSqlAction[A, B, C]): Try[A] = {
     try {
       Success(Await.result(db.run(statement), Duration("5 second"))) 
     } catch {
       case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
     }
    }
    
    def execute[A, S <: slick.dbio.NoStream, E <: slick.dbio.Effect](statement: DBIOAction[A, S, E]) = {
      try {
        Await.result(db.run(statement), Duration("5 second")) 
      } catch {
        case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
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
        case e: Exception => logger.warn(s"Error while executing statement ${statement}" + e); Failure(e)
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

object JDBCDbSession {
  def getNewJDBCDbSession(ds: HikariDataSource, sqlDialiect: ScraySQLDialect) = {
    Try(new JDBCDbSession(ds, sqlDialiect))
  }
  
  def getNewJDBCDbSession(jdbcURL: String, username: String, password: String) = {
    Try(new JDBCDbSession(jdbcURL, username, password))
  }
}