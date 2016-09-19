package scray.cassandra.sync

import scala.collection.JavaConverters._

import com.datastax.driver.core.ResultSet
import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.Insert
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.typesafe.scalalogging.slf4j.LazyLogging

import scray.cassandra.sync.CassandraImplementation.genericCassandraColumnImplicit
import scray.querying.sync.ArbitrarylyTypedRows
import scray.querying.sync.Column
import scray.querying.sync.DBColumnImplementation
import scray.querying.sync.DbSession
import scray.querying.sync.JobInfo
import scray.querying.sync.Table
import scala.util.Try
import scala.util.Failure
import scray.cassandra.util.CassandraUtils

/**
 * Find a consensus about the start time of a job.
 * Start time is the time of the first consumed element. This can be an attribute of the element.
 * It is assumed that all elements, consumed by one instance are ordered in an increasing order. (The first element is the oldest element)
 * Also other ordinal attributes can be use.
 */
object StartTimeDetector extends LazyLogging {

  val startConsensusTable = new Table("silidx", "startconsensus", new StartConsensusRow)
  
  /**
   * Create keyspaces and tables if needed.
   */
  def init(job: JobInfo[Statement, Insert, ResultSet], dbSession: DbSession[Statement, Insert, ResultSet]) = {
    CassandraUtils.createKeyspaceCreationStatement(startConsensusTable).map { statement => dbSession.execute(statement) }
    CassandraUtils.createTableStatement(startConsensusTable).map { statement => dbSession.execute(statement) }
  }

  def allNodesVoted(job: JobInfo[Statement, Insert, ResultSet], dbSession: DbSession[Statement, Insert, ResultSet]): Option[Long] = {
    val votes = QueryBuilder.select.all().from(startConsensusTable.keySpace, startConsensusTable.tableName).where(
      QueryBuilder.eq(startConsensusTable.columns.jobname.name, job.name))

    // Check if all worker voted
    val allWorkerVoted = dbSession.execute(votes)
      .map { _.all() }.recover {
        case e => { logger.error(s"DB error while fetching all element of the vote for ${job.name}. Error: ${e.getMessage}"); throw e }
      }.toOption.map { rows =>
        if (rows.size() > 0) {
          ((rows.get(0).getInt(startConsensusTable.columns.numberOfWorkers.name) <= rows.size()), rows)
        } else {
          (false, rows)
        }
      }.map { voteResult =>
        if (voteResult._1) {
          logger.debug("All nodes sended first element time")
          Some(getMinTime(voteResult._2))
        } else {
          logger.debug("Not all workes pulished the first element time. Try it again later")
          None
        }
      }
      
    return allWorkerVoted.flatten
  }


  def publishLocalStartTime(job: JobInfo[Statement, Insert, ResultSet], dbSession: DbSession[Statement, Insert, ResultSet], time: Long): Try[Boolean] = {
    val statement = job.numberOfWorkers.map { numWorkers => 
     QueryBuilder.insertInto(startConsensusTable.keySpace, startConsensusTable.tableName)
          .value(startConsensusTable.columns.jobname.name, job.name)
          .value(startConsensusTable.columns.time.name, time)  
          .value(startConsensusTable.columns.numberOfWorkers.name, numWorkers)      
    }
    
    statement match {
      case Some(statement) => Try(dbSession.insert(statement).isSuccess)
      case None => logger.warn("No numberOfWorkers configured! No time update"); throw new RuntimeException("No numberOfWorkers configured! No time update")
    }
  }
  
  /**
   * Get the time of the first element.
   */
  def getMinTime(times: java.util.List[Row]): Long = {

    times.asScala.foldLeft(Long.MaxValue)((min, row) => {
      if (row.getLong(startConsensusTable.columns.time.name) < min) {
        row.getLong(startConsensusTable.columns.time.name)
      } else {
        min
      }
    })
  }
  

  class StartConsensusRow(implicit colString: DBColumnImplementation[String],
                          colLong: DBColumnImplementation[Long],
                          colInt: DBColumnImplementation[Int]) extends ArbitrarylyTypedRows {

    val jobname = new Column[String]("jobname")
    val time = new Column[Long]("time")
    val numberOfWorkers = new Column[Int]("numWorkers")

    override val columns = jobname :: time :: numberOfWorkers :: Nil
    override val primaryKey = s"(${jobname.name}, ${time.name})"
    override val indexes: Option[List[String]] = None
  }

}



