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
import scray.jdbc.extractors.ScrayHiveDialect
import slick.dbio.Effect
import slick.dbio.NoStream
import java.sql.DriverManager

class JDBCDbSessionImpl(val ds: HikariDataSource, val metadataConnection: Connection, val sqlDialiect: ScraySQLDialect) extends JDBCDbSession with LazyLogging {
  
  def this(ds: HikariDataSource, sqlDialiect: ScraySQLDialect) = this(ds, ds.getConnection, sqlDialiect)
  
  def this(jdbcURL: String, sqlDialect: ScraySQLDialect, username: String, password: String) = {
      this({
        val config = new HikariConfig()
        config.setDriverClassName(sqlDialect.DRIVER_CLASS_NAME)
        config.setJdbcUrl(jdbcURL)
        config.setUsername(username)
        config.setPassword(password)
        config.setMaximumPoolSize(3)

        if(sqlDialect.getName.toUpperCase().startsWith(ScrayHiveDialect.name)) {
          class HiveDataSource extends HikariDataSource {
            override def getConnection = synchronized {
              DriverManager.getConnection(jdbcURL, username, password)
            }
          }
          
          new HiveDataSource
        } else {
          new HikariDataSource(config)
        } 
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