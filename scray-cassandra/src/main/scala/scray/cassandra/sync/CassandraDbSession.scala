package scray.cassandra.sync

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.BatchStatement
import scala.util.Try
import scala.util.Failure
import scala.util.Success
import scray.querying.sync.DbSession
import com.typesafe.scalalogging.slf4j.LazyLogging
import scray.querying.sync.StatementExecutionError
import com.datastax.driver.core.Cluster

class CassandraDbSession(val cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet](cassandraSession.getCluster.getMetadata.getAllHosts().iterator().next.getAddress.toString) with LazyLogging{
  
  def this(host: String) = {
    this(Cluster.builder().addContactPoint(host).build().connect())
  }
  
  override def execute(statement: String): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: Statement): Try[ResultSet] = {
      try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def insert(statement: Insert): Try[ResultSet] = {
      try {
        logger.debug("Insert " + statement)
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Condition was false"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }

    def execute(statement: SimpleStatement): Try[ResultSet] = {
       try {
        val result = cassandraSession.execute(statement)
        if(result.wasApplied()) {
         Success(result)
       } else {
         Failure(new StatementExecutionError(s"It was not possible to execute statement: ${statement}. Error: ${result.getExecutionInfo}"))
       }
      } catch {
        case e: Exception => logger.error(s"Error while executing statement ${statement}" + e); Failure(e)
      }
    }
    
    def printStatement(s: Statement): String = {
      s match {
         case bStatement: BatchStatement => "It is currently not possible to execute : " + bStatement.getStatements
         case _                          => "It is currently not possible to execute : " + s
      }
    }
}