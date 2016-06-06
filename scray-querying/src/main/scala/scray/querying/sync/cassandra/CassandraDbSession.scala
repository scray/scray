package scray.querying.sync.cassandra

import com.websudos.phantom.CassandraPrimitive
import scray.querying.sync.types.DBColumnImplementation
import java.util.{ Iterator => JIterator }
import scala.annotation.tailrec
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.RegularStatement
import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import java.util.ArrayList
import scala.collection.mutable.ArrayBuffer
import scray.querying.sync.types.SyncTableBasicClasses.SyncTableRowEmpty
import scala.collection.mutable.ListBuffer
import com.datastax.driver.core.BatchStatement
import scray.querying.sync.types.State.State
import com.datastax.driver.core.querybuilder.Update.Where
import com.datastax.driver.core.querybuilder.Update.Conditions
import scala.util.Try
import scray.querying.sync.RunningJobExistsException
import scray.querying.sync.NoRunningJobExistsException
import scray.querying.sync.StatementExecutionError
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConversions
import scray.querying.description.TableIdentifier
import scala.collection.mutable.HashSet
import com.datastax.driver.core.querybuilder.Select
import scray.querying.sync.types.VoidTable
import scray.querying.sync.types.Table
import scray.querying.sync.types.SyncTable
import scray.querying.sync.types.State
import scray.querying.sync.types.RowWithValue
import scray.querying.sync.types.JobLockTable
import scray.querying.sync.types.DbSession
import scray.querying.sync.types.ColumnWithValue
import scray.querying.sync.types.AbstractRow
import scray.common.serialization.BatchID
import scray.querying.sync.types.SyncTableBasicClasses.JobLockTable
import scray.querying.sync.JobInfo
import java.util.concurrent.TimeUnit
import scala.None
import com.typesafe.scalalogging.slf4j.LazyLogging

class CassandraDbSession(val cassandraSession: Session) extends DbSession[Statement, Insert, ResultSet, CassandraMetadata](cassandraSession.getCluster.getMetadata.getAllHosts().iterator().next.getAddress.toString) with LazyLogging{
      override def execute(statement: String): Try[ResultSet] = {
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

    def execute(statement: Statement): Try[ResultSet] = {
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

    def insert(statement: Insert): Try[ResultSet] = {
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